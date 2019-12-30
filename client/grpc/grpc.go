// Package grpc provides a gRPC client
package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/client/selector"
	raw "github.com/micro/go-micro/codec/bytes"
	"github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	gmetadata "google.golang.org/grpc/metadata"
)

type grpcClient struct {
	once sync.Once
	opts client.Options
	pool *pool
}

func init() {
	encoding.RegisterCodec(wrapCodec{jsonCodec{}})
	encoding.RegisterCodec(wrapCodec{protoCodec{}})
	encoding.RegisterCodec(wrapCodec{bytesCodec{}})
}

// secure returns the dial option for whether its a secure or insecure connection
func (g *grpcClient) secure() grpc.DialOption {
	if g.opts.Context != nil {
		if v := g.opts.Context.Value(tlsAuth{}); v != nil {
			tls := v.(*tls.Config)
			creds := credentials.NewTLS(tls)
			return grpc.WithTransportCredentials(creds)
		}
	}
	return grpc.WithInsecure()
}

// next返回一个函数
func (g *grpcClient) next(request client.Request, opts client.CallOptions) (selector.Next, error) {
	// 服务名称，例如go.micro.srv.gretter
	service := request.Service()

	// get proxy
	// 如果设置了代理则服务名称改成代理名称，即代理服务
	if prx := os.Getenv("MICRO_PROXY"); len(prx) > 0 {
		service = prx
	}

	// get proxy address
	// 如果配置了代理地址，则将服务地址改成代理服务的地址
	if prx := os.Getenv("MICRO_PROXY_ADDRESS"); len(prx) > 0 {
		opts.Address = []string{prx}
	}

	// return remote address
	// 如果有地址说明不能走服务发现，而要走第一个地址
	// 所以返回的函数，只返回第一个地址
	if len(opts.Address) > 0 {
		return func() (*registry.Node, error) {
			return &registry.Node{
				Address: opts.Address[0],
			}, nil
		}, nil
	}

	// get next nodes from the selector
	next, err := g.opts.Selector.Select(service, opts.SelectOptions...)
	if err != nil {
		if err == selector.ErrNotFound {
			return nil, errors.InternalServerError("go.micro.client", "service %s: %s", service, err.Error())
		}
		return nil, errors.InternalServerError("go.micro.client", "error selecting %s node: %s", service, err.Error())
	}

	return next, nil
}

func (g *grpcClient) call(ctx context.Context, node *registry.Node, req client.Request, rsp interface{}, opts client.CallOptions) error {
	address := node.Address

	header := make(map[string]string)
	// 从context中获取头(元)信息
	if md, ok := metadata.FromContext(ctx); ok {
		for k, v := range md {
			header[k] = v
		}
	}

	// set timeout in nanoseconds
	// 服务端处理的超时时间
	header["timeout"] = fmt.Sprintf("%d", opts.RequestTimeout)
	// set the content type for the request
	header["x-content-type"] = req.ContentType()

	// grpc格式的md
	md := gmetadata.New(header)
	// 构造一个保存头信息的context
	ctx = gmetadata.NewOutgoingContext(ctx, md)

	// 根据content-type设置 编解码 方式
	cf, err := g.newGRPCCodec(req.ContentType())
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}

	maxRecvMsgSize := g.maxRecvMsgSizeValue()
	maxSendMsgSize := g.maxSendMsgSizeValue()

	var grr error

	grpcDialOptions := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.ForceCodec(cf)),
		grpc.WithTimeout(opts.DialTimeout),
		g.secure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxRecvMsgSize),
			grpc.MaxCallSendMsgSize(maxSendMsgSize),
		),
	}

	if opts := g.getGrpcDialOptions(); opts != nil {
		grpcDialOptions = append(grpcDialOptions, opts...)
	}

	cc, err := g.pool.getConn(address, grpcDialOptions...)
	if err != nil {
		return errors.InternalServerError("go.micro.client", fmt.Sprintf("Error sending request: %v", err))
	}
	defer func() {
		// defer execution of release
		// 释放连接到连接池，如果连接池满或者grpc调用出错，则连接被关闭
		g.pool.release(address, cc, grr)
	}()

	ch := make(chan error, 1)

	go func() {
		grpcCallOptions := []grpc.CallOption{grpc.CallContentSubtype(cf.Name())}
		if opts := g.getGrpcCallOptions(); opts != nil {
			grpcCallOptions = append(grpcCallOptions, opts...)
		}
		// 构造GRPC方法路由，发起GRPC请求
		err := cc.Invoke(ctx, methodToGRPC(req.Service(), req.Endpoint()), req.Body(), rsp, grpcCallOptions...)
		ch <- microError(err)
	}()

	select {
	case err := <-ch:
		grr = err
	case <-ctx.Done():
		grr = ctx.Err()
	}

	return grr
}

func (g *grpcClient) stream(ctx context.Context, node *registry.Node, req client.Request, opts client.CallOptions) (client.Stream, error) {
	address := node.Address

	header := make(map[string]string)
	if md, ok := metadata.FromContext(ctx); ok {
		for k, v := range md {
			header[k] = v
		}
	}

	// set timeout in nanoseconds
	header["timeout"] = fmt.Sprintf("%d", opts.RequestTimeout)
	// set the content type for the request
	header["x-content-type"] = req.ContentType()

	md := gmetadata.New(header)
	ctx = gmetadata.NewOutgoingContext(ctx, md)

	cf, err := g.newGRPCCodec(req.ContentType())
	if err != nil {
		return nil, errors.InternalServerError("go.micro.client", err.Error())
	}

	var dialCtx context.Context
	var cancel context.CancelFunc
	if opts.DialTimeout >= 0 {
		dialCtx, cancel = context.WithTimeout(ctx, opts.DialTimeout)
	} else {
		dialCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	wc := wrapCodec{cf}

	grpcDialOptions := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.ForceCodec(wc)),
		g.secure(),
	}

	if opts := g.getGrpcDialOptions(); opts != nil {
		grpcDialOptions = append(grpcDialOptions, opts...)
	}

	cc, err := grpc.DialContext(dialCtx, address, grpcDialOptions...)
	if err != nil {
		return nil, errors.InternalServerError("go.micro.client", fmt.Sprintf("Error sending request: %v", err))
	}

	desc := &grpc.StreamDesc{
		StreamName:    req.Service() + req.Endpoint(),
		ClientStreams: true,
		ServerStreams: true,
	}

	grpcCallOptions := []grpc.CallOption{}
	if opts := g.getGrpcCallOptions(); opts != nil {
		grpcCallOptions = append(grpcCallOptions, opts...)
	}
	st, err := cc.NewStream(ctx, desc, methodToGRPC(req.Service(), req.Endpoint()), grpcCallOptions...)
	if err != nil {
		return nil, errors.InternalServerError("go.micro.client", fmt.Sprintf("Error creating stream: %v", err))
	}

	codec := &grpcCodec{
		s: st,
		c: wc,
	}

	// set request codec
	if r, ok := req.(*grpcRequest); ok {
		r.codec = codec
	}

	rsp := &response{
		conn:   cc,
		stream: st,
		codec:  cf,
		gcodec: codec,
	}

	return &grpcStream{
		context:  ctx,
		request:  req,
		response: rsp,
		stream:   st,
		conn:     cc,
	}, nil
}

func (g *grpcClient) poolMaxStreams() int {
	if g.opts.Context == nil {
		return DefaultPoolMaxStreams
	}
	v := g.opts.Context.Value(poolMaxStreams{})
	if v == nil {
		return DefaultPoolMaxStreams
	}
	return v.(int)
}

func (g *grpcClient) poolMaxIdle() int {
	if g.opts.Context == nil {
		return DefaultPoolMaxIdle
	}
	v := g.opts.Context.Value(poolMaxIdle{})
	if v == nil {
		return DefaultPoolMaxIdle
	}
	return v.(int)
}

func (g *grpcClient) maxRecvMsgSizeValue() int {
	if g.opts.Context == nil {
		return DefaultMaxRecvMsgSize
	}
	v := g.opts.Context.Value(maxRecvMsgSizeKey{})
	if v == nil {
		return DefaultMaxRecvMsgSize
	}
	return v.(int)
}

func (g *grpcClient) maxSendMsgSizeValue() int {
	if g.opts.Context == nil {
		return DefaultMaxSendMsgSize
	}
	v := g.opts.Context.Value(maxSendMsgSizeKey{})
	if v == nil {
		return DefaultMaxSendMsgSize
	}
	return v.(int)
}

func (g *grpcClient) newGRPCCodec(contentType string) (encoding.Codec, error) {
	// 先检查是否设置过contentType采用哪种Codec
	codecs := make(map[string]encoding.Codec)
	if g.opts.Context != nil {
		if v := g.opts.Context.Value(codecsKey{}); v != nil {
			codecs = v.(map[string]encoding.Codec)
		}
	}
	if c, ok := codecs[contentType]; ok {
		return wrapCodec{c}, nil
	}
	if c, ok := defaultGRPCCodecs[contentType]; ok {
		return wrapCodec{c}, nil
	}
	return nil, fmt.Errorf("Unsupported Content-Type: %s", contentType)
}

func (g *grpcClient) Init(opts ...client.Option) error {
	size := g.opts.PoolSize
	ttl := g.opts.PoolTTL

	for _, o := range opts {
		o(&g.opts)
	}

	// update pool configuration if the options changed
	if size != g.opts.PoolSize || ttl != g.opts.PoolTTL {
		g.pool.Lock()
		g.pool.size = g.opts.PoolSize
		g.pool.ttl = int64(g.opts.PoolTTL.Seconds())
		g.pool.Unlock()
	}

	return nil
}

func (g *grpcClient) Options() client.Options {
	return g.opts
}

func (g *grpcClient) NewMessage(topic string, msg interface{}, opts ...client.MessageOption) client.Message {
	return newGRPCEvent(topic, msg, g.opts.ContentType, opts...)
}

func (g *grpcClient) NewRequest(service, method string, req interface{}, reqOpts ...client.RequestOption) client.Request {
	return newGRPCRequest(service, method, req, g.opts.ContentType, reqOpts...)
}

func (g *grpcClient) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	// make a copy of call opts
	callOpts := g.opts.CallOptions
	// 调用可以更改配置项的函数参数
	for _, opt := range opts {
		opt(&callOpts)
	}

	// 获取一个函数，该函数根据一定策略挑选下一个服务节点
	next, err := g.next(req, callOpts)
	if err != nil {
		return err
	}

	// check if we already have a deadline
	// 当前context是否设置过到哪个时间点结束(请求完成或者被cancel)
	// 如果设置过就使用context的截止时间，否则使用默认配置的超时时间
	d, ok := ctx.Deadline()
	if !ok {
		// no deadline so we create a new one
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, callOpts.RequestTimeout)
		defer cancel()
	} else {
		// got a deadline so no need to setup context
		// but we need to set the timeout we pass along
		// 如果设置了截止时间，则将 当前时间到截止时间 之间的时间长度作为请求的超时时间
		// 意思是，如果到了截止时间还没完成，那么结果就是超时
		opt := client.WithRequestTimeout(time.Until(d))
		opt(&callOpts)
	}

	// should we noop right here?
	select {
	case <-ctx.Done():
		return errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
	default:
	}

	// make copy of call method
	gcall := g.call

	// wrap the call in reverse
	// 出栈调用CallWrappers
	for i := len(callOpts.CallWrappers); i > 0; i-- {
		gcall = callOpts.CallWrappers[i-1](gcall)
	}

	// return errors.New("go.micro.client", "request timeout", 408)
	call := func(i int) error {
		// call backoff first. Someone may want an initial start delay
		// 一开始就判断需要等待多久，第一次不需要等待
		t, err := callOpts.Backoff(ctx, req, i)
		if err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}

		// only sleep if greater than 0
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		// select next node
		// 算法选择节点
		node, err := next()
		service := req.Service()
		if err != nil {
			if err == selector.ErrNotFound {
				return errors.InternalServerError("go.micro.client", "service %s: %s", service, err.Error())
			}
			return errors.InternalServerError("go.micro.client", "error selecting %s node: %s", service, err.Error())
		}

		// make the call
		// 发起请求
		err = gcall(ctx, node, req, rsp, callOpts)
		g.opts.Selector.Mark(service, node, err)
		return err
	}

	ch := make(chan error, callOpts.Retries+1)
	var gerr error

	for i := 0; i <= callOpts.Retries; i++ {
		go func(i int) {
			ch <- call(i)
		}(i)

		select {
		case <-ctx.Done():
			return errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
		case err := <-ch:
			// if the call succeeded lets bail early
			if err == nil {
				return nil
			}

			// 是否要重试，错误和false都不重试
			retry, rerr := callOpts.Retry(ctx, req, i, err)
			if rerr != nil {
				return rerr
			}

			// false不重试
			if !retry {
				return err
			}

			gerr = err
		}
	}

	return gerr
}

func (g *grpcClient) Stream(ctx context.Context, req client.Request, opts ...client.CallOption) (client.Stream, error) {
	// make a copy of call opts
	callOpts := g.opts.CallOptions
	for _, opt := range opts {
		opt(&callOpts)
	}

	next, err := g.next(req, callOpts)
	if err != nil {
		return nil, err
	}

	// #200 - streams shouldn't have a request timeout set on the context

	// should we noop right here?
	select {
	case <-ctx.Done():
		return nil, errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
	default:
	}

	call := func(i int) (client.Stream, error) {
		// call backoff first. Someone may want an initial start delay
		t, err := callOpts.Backoff(ctx, req, i)
		if err != nil {
			return nil, errors.InternalServerError("go.micro.client", err.Error())
		}

		// only sleep if greater than 0
		if t.Seconds() > 0 {
			time.Sleep(t)
		}

		node, err := next()
		service := req.Service()
		if err != nil {
			if err == selector.ErrNotFound {
				return nil, errors.InternalServerError("go.micro.client", "service %s: %s", service, err.Error())
			}
			return nil, errors.InternalServerError("go.micro.client", "error selecting %s node: %s", service, err.Error())
		}

		stream, err := g.stream(ctx, node, req, callOpts)
		g.opts.Selector.Mark(service, node, err)
		return stream, err
	}

	type response struct {
		stream client.Stream
		err    error
	}

	ch := make(chan response, callOpts.Retries+1)
	var grr error

	for i := 0; i <= callOpts.Retries; i++ {
		go func(i int) {
			s, err := call(i)
			ch <- response{s, err}
		}(i)

		select {
		case <-ctx.Done():
			return nil, errors.New("go.micro.client", fmt.Sprintf("%v", ctx.Err()), 408)
		case rsp := <-ch:
			// if the call succeeded lets bail early
			if rsp.err == nil {
				return rsp.stream, nil
			}

			retry, rerr := callOpts.Retry(ctx, req, i, err)
			if rerr != nil {
				return nil, rerr
			}

			if !retry {
				return nil, rsp.err
			}

			grr = rsp.err
		}
	}

	return nil, grr
}

func (g *grpcClient) Publish(ctx context.Context, p client.Message, opts ...client.PublishOption) error {
	// 从context中获取元信息
	var options client.PublishOptions
	for _, o := range opts {
		o(&options)
	}

	md, ok := metadata.FromContext(ctx)
	if !ok {
		md = make(map[string]string)
	}
	md["Content-Type"] = p.ContentType()
	// 获取编解码对象
	md["Micro-Topic"] = p.Topic()
	cf, err := g.newGRPCCodec(p.ContentType())
	if err != nil {
		return errors.InternalServerError("go.micro.client", err.Error())
	}
	var body []byte

	// passed in raw data
	if d, ok := p.Payload().(*raw.Frame); ok {
		body = d.Data
	} else {
		// set the body
		// 编码自定义消息数据对象
		b, err := cf.Marshal(p.Payload())
		if err != nil {
			return errors.InternalServerError("go.micro.client", err.Error())
		}
		body = b
	}

	// 连接到broker，只有第一次调用的时候执行
	// TODO：这个时候还会启动broker server， why？
	g.once.Do(func() {
		g.opts.Broker.Connect()
	})

	topic := p.Topic()

	// get proxy topic
	if prx := os.Getenv("MICRO_PROXY"); len(prx) > 0 {
		options.Exchange = prx
	}

	// get the exchange
	if len(options.Exchange) > 0 {
		topic = options.Exchange
	}

	return g.opts.Broker.Publish(topic, &broker.Message{
		Header: md,
		Body:   body,
	})
}

func (g *grpcClient) String() string {
	return "grpc"
}

func (g *grpcClient) getGrpcDialOptions() []grpc.DialOption {
	if g.opts.CallOptions.Context == nil {
		return nil
	}

	v := g.opts.CallOptions.Context.Value(grpcDialOptions{})

	if v == nil {
		return nil
	}

	opts, ok := v.([]grpc.DialOption)

	if !ok {
		return nil
	}

	return opts
}

func (g *grpcClient) getGrpcCallOptions() []grpc.CallOption {
	if g.opts.CallOptions.Context == nil {
		return nil
	}

	v := g.opts.CallOptions.Context.Value(grpcCallOptions{})

	if v == nil {
		return nil
	}

	opts, ok := v.([]grpc.CallOption)

	if !ok {
		return nil
	}

	return opts
}

func newClient(opts ...client.Option) client.Client {
	options := client.NewOptions()
	// default content type for grpc
	options.ContentType = "application/grpc+proto"

	// 调用可修改Options的函数参数
	for _, o := range opts {
		o(&options)
	}

	rc := &grpcClient{
		once: sync.Once{},
		opts: options,
	}

	rc.pool = newPool(options.PoolSize, options.PoolTTL, rc.poolMaxIdle(), rc.poolMaxStreams())

	c := client.Client(rc)

	// wrap in reverse
	// 出栈调用wrapper
	for i := len(options.Wrappers); i > 0; i-- {
		c = options.Wrappers[i-1](c)
	}

	return c
}

func NewClient(opts ...client.Option) client.Client {
	return newClient(opts...)
}
