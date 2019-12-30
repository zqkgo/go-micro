// Package grpc provides a grpc server
package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"reflect"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/codec"
	"github.com/micro/go-micro/errors"
	meta "github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-micro/util/addr"
	mgrpc "github.com/micro/go-micro/util/grpc"
	"github.com/micro/go-micro/util/log"
	mnet "github.com/micro/go-micro/util/net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var (
	// DefaultMaxMsgSize define maximum message size that server can send
	// or receive.  Default value is 4MB.
	DefaultMaxMsgSize = 1024 * 1024 * 4
)

const (
	defaultContentType = "application/grpc"
)

type grpcServer struct {
	// 提取、保存服务名称到服务信息的映射
	rpc *rServer
	// grpc server对象，实际处理grpc请求的对象
	srv  *grpc.Server
	exit chan chan error
	// 通过
	wg *sync.WaitGroup

	sync.RWMutex
	opts server.Options
	// 服务名 -> 服务handler，即一个server可以同时提供多个服务。
	// 注册服务时，根据该成员找到所有handler的endpoints集合
	handlers map[string]server.Handler
	// 遍历每个subscriber注册到broker
	subscribers map[*subscriber][]broker.Subscriber
	// marks the serve as started
	started bool
	// used for first registration
	registered bool
}

func init() {
	encoding.RegisterCodec(wrapCodec{jsonCodec{}})
	encoding.RegisterCodec(wrapCodec{protoCodec{}})
	encoding.RegisterCodec(wrapCodec{bytesCodec{}})
}

func newGRPCServer(opts ...server.Option) server.Server {
	// 创建server.Options对象，调用修改方法，赋默认值
	options := newOptions(opts...)

	// create a grpc server
	srv := &grpcServer{
		opts: options,
		rpc: &rServer{
			serviceMap: make(map[string]*service),
		},
		handlers:    make(map[string]server.Handler),
		subscribers: make(map[*subscriber][]broker.Subscriber),
		exit:        make(chan chan error),
		wg:          wait(options.Context),
	}

	// configure the grpc server
	// 根据默认或自定义配置构造grpc server对象
	srv.configure()

	return srv
}

type grpcRouter struct {
	h func(context.Context, server.Request, interface{}) error
	m func(context.Context, server.Message) error
}

func (r grpcRouter) ProcessMessage(ctx context.Context, msg server.Message) error {
	return r.m(ctx, msg)
}

func (r grpcRouter) ServeRequest(ctx context.Context, req server.Request, rsp server.Response) error {
	return r.h(ctx, req, rsp)
}

// 根据默认或自定义配置构造grpc server对象
// 配置项包括message大小最大值、TLS配置等等
func (g *grpcServer) configure(opts ...server.Option) {
	// Don't reprocess where there's no config
	if len(opts) == 0 && g.srv != nil {
		return
	}

	for _, o := range opts {
		o(&g.opts)
	}

	maxMsgSize := g.getMaxMsgSize()
	// grpc配置项
	gopts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
		// 如果grpc server接收到请求后，发现服务或方法未注册，会调用此配置
		// 截止注释时间，所有的服务都会被该配置处理
		grpc.UnknownServiceHandler(g.handler),
	}
	// TLS配置
	if creds := g.getCredentials(); creds != nil {
		gopts = append(gopts, grpc.Creds(creds))
	}
	// 其他配置项（数组）
	if opts := g.getGrpcOptions(); opts != nil {
		gopts = append(gopts, opts...)
	}

	g.srv = grpc.NewServer(gopts...)
}

// 获取server能发送或接受的最大message大小，如果context中没有则使用默认大小
func (g *grpcServer) getMaxMsgSize() int {
	if g.opts.Context == nil {
		return DefaultMaxMsgSize
	}
	s, ok := g.opts.Context.Value(maxMsgSizeKey{}).(int)
	if !ok {
		return DefaultMaxMsgSize
	}
	return s
}

func (g *grpcServer) getCredentials() credentials.TransportCredentials {
	if g.opts.Context != nil {
		if v := g.opts.Context.Value(tlsAuth{}); v != nil {
			tls := v.(*tls.Config)
			return credentials.NewTLS(tls)
		}
	}
	return nil
}

func (g *grpcServer) getGrpcOptions() []grpc.ServerOption {
	if g.opts.Context == nil {
		return nil
	}

	v := g.opts.Context.Value(grpcOptions{})

	if v == nil {
		return nil
	}
	// 使用type assertion提取interface的concrete type
	opts, ok := v.([]grpc.ServerOption)

	if !ok {
		return nil
	}

	return opts
}

// 自定义的未知服务或方法的回调，注册给grpc
// 当访问未注册的服务或方法时调用此方法
func (g *grpcServer) handler(srv interface{}, stream grpc.ServerStream) error {
	if g.wg != nil {
		g.wg.Add(1)
		defer g.wg.Done()
	}

	// 获取完整的方法路径，例如：/go.micro.srv.funcsave.Save/Categories
	fullMethod, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Errorf(codes.Internal, "method does not exist in context")
	}

	// 提取出服务名和方法名
	serviceName, methodName, err := mgrpc.ServiceMethod(fullMethod)
	if err != nil {
		return status.New(codes.InvalidArgument, err.Error()).Err()
	}

	// get grpc metadata
	gmd, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		gmd = metadata.MD{}
	}

	// copy the metadata to go-micro.metadata
	// 复制一份元信息以便对之进行修改
	md := meta.Metadata{}
	for k, v := range gmd {
		md[k] = strings.Join(v, ", ")
	}

	// timeout for server deadline
	to := md["timeout"]

	// get content type
	ct := defaultContentType
	if ctype, ok := md["x-content-type"]; ok {
		ct = ctype
	}

	delete(md, "x-content-type")
	delete(md, "timeout")

	// create new context
	// 以stream.Context()为parent构造一个包含头信息的context
	ctx := meta.NewContext(stream.Context(), md)

	// get peer from context
	// 如果stream.Context()包含发送请求的peer信息，
	// 以ctx为parent构造包含peer信息的context
	if p, ok := peer.FromContext(stream.Context()); ok {
		md["Remote"] = p.Addr.String()
		ctx = peer.NewContext(ctx, p)
	}

	// set the timeout if we have it
	// 如果有超时时间则构造包含超时时间的context
	if len(to) > 0 {
		if n, err := strconv.ParseUint(to, 10, 64); err == nil {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(n))
			defer cancel()
		}
	}

	// process via router
	// 如果设置了路由Router则使用Router处理请求
	if g.opts.Router != nil {
		// 根据content-type获取编解码方式
		cc, err := g.newGRPCCodec(ct)
		if err != nil {
			return errors.InternalServerError("go.micro.server", err.Error())
		}
		// codec使用stream对象读写消息
		codec := &grpcCodec{
			method:   fmt.Sprintf("%s.%s", serviceName, methodName),
			endpoint: fmt.Sprintf("%s.%s", serviceName, methodName),
			target:   g.opts.Name,
			s:        stream,
			c:        cc,
		}

		// create a client.Request
		request := &rpcRequest{
			service:     mgrpc.ServiceFromMethod(fullMethod),
			contentType: ct,
			method:      fmt.Sprintf("%s.%s", serviceName, methodName),
			codec:       codec,
		}

		response := &rpcResponse{
			header: make(map[string]string),
			codec:  codec,
		}

		// create a wrapped function
		handler := func(ctx context.Context, req server.Request, rsp interface{}) error {
			return g.opts.Router.ServeRequest(ctx, req, rsp.(server.Response))
		}

		// execute the wrapper for it
		for i := len(g.opts.HdlrWrappers); i > 0; i-- {
			handler = g.opts.HdlrWrappers[i-1](handler)
		}

		r := grpcRouter{h: handler}

		// serve the actual request using the request router
		if err := r.ServeRequest(ctx, request, response); err != nil {
			return status.Errorf(codes.Internal, err.Error())
		}

		return nil
	}

	// process the standard request flow
	// 根据服务名获取服务对象
	g.rpc.mu.Lock()
	service := g.rpc.serviceMap[serviceName]
	g.rpc.mu.Unlock()

	if service == nil {
		return status.New(codes.Unimplemented, fmt.Sprintf("unknown service %s", serviceName)).Err()
	}

	// 根据方法名获取方法对象
	mtype := service.method[methodName]
	if mtype == nil {
		return status.New(codes.Unimplemented, fmt.Sprintf("unknown service %s.%s", serviceName, methodName)).Err()
	}

	// process unary
	// 处理unary类型的请求
	if !mtype.stream {
		return g.processRequest(stream, service, mtype, ct, ctx)
	}

	// process stream
	// 处理stream类型的请求
	return g.processStream(stream, service, mtype, ct, ctx)
}

func (g *grpcServer) processRequest(stream grpc.ServerStream, service *service, mtype *methodType, ct string, ctx context.Context) error {
	for {
		var argv, replyv reflect.Value

		// Decode the argument value.
		argIsValue := false // if true, need to indirect before calling.
		// 创建指向 入参类型零值 的指针
		if mtype.ArgType.Kind() == reflect.Ptr {
			argv = reflect.New(mtype.ArgType.Elem())
		} else {
			argv = reflect.New(mtype.ArgType)
			argIsValue = true
		}

		// Unmarshal request
		// 接收message并赋值给argv
		if err := stream.RecvMsg(argv.Interface()); err != nil {
			return err
		}

		if argIsValue {
			argv = argv.Elem()
		}

		// reply value
		// 创建指向 出参类型零值 的指针
		replyv = reflect.New(mtype.ReplyType.Elem())

		// function的第一个参数是方法的receiver的值
		function := mtype.method.Func
		var returnValues []reflect.Value

		// 获取编解码对象并对入参编码
		cc, err := g.newGRPCCodec(ct)
		if err != nil {
			return errors.InternalServerError("go.micro.server", err.Error())
		}
		b, err := cc.Marshal(argv.Interface())
		if err != nil {
			return err
		}

		// create a client.Request
		// 构造请求对象，没用(9d559848c2ea185c6f54206012686adfb36d0fb9)?
		r := &rpcRequest{
			service:     g.opts.Name,
			contentType: ct,
			method:      fmt.Sprintf("%s.%s", service.name, mtype.method.Name),
			body:        b,
			payload:     argv.Interface(),
		}

		// define the handler func
		fn := func(ctx context.Context, req server.Request, rsp interface{}) (err error) {
			defer func() {
				if r := recover(); r != nil {
					log.Log("panic recovered: ", r)
					log.Logf(string(debug.Stack()))
					err = errors.InternalServerError("go.micro.server", "panic recovered: %v", r)
				}
			}()
			returnValues = function.Call([]reflect.Value{service.rcvr, mtype.prepareContext(ctx), reflect.ValueOf(argv.Interface()), reflect.ValueOf(rsp)})

			// The return value for the method is an error.
			if rerr := returnValues[0].Interface(); rerr != nil {
				err = rerr.(error)
			}

			return err
		}

		// wrap the handler func
		// 使用配置的若干封装函数封装起来
		for i := len(g.opts.HdlrWrappers); i > 0; i-- {
			fn = g.opts.HdlrWrappers[i-1](fn)
		}

		statusCode := codes.OK
		statusDesc := ""

		// execute the handler
		// 执行函数，根据错误类型设置错误码和错误描述
		if appErr := fn(ctx, r, replyv.Interface()); appErr != nil {
			if err, ok := appErr.(*rpcError); ok {
				statusCode = err.code
				statusDesc = err.desc
			} else if err, ok := appErr.(*errors.Error); ok {
				statusCode = microError(err)
				statusDesc = appErr.Error()
			} else {
				statusCode = convertCode(appErr)
				statusDesc = appErr.Error()
			}
			return status.New(statusCode, statusDesc).Err()
		}
		// 发送响应数据到调用方
		if err := stream.SendMsg(replyv.Interface()); err != nil {
			return err
		}
		return status.New(statusCode, statusDesc).Err()
	}
}

func (g *grpcServer) processStream(stream grpc.ServerStream, service *service, mtype *methodType, ct string, ctx context.Context) error {
	opts := g.opts

	r := &rpcRequest{
		service:     opts.Name,
		contentType: ct,
		method:      fmt.Sprintf("%s.%s", service.name, mtype.method.Name),
		stream:      true,
	}

	ss := &rpcStream{
		request: r,
		s:       stream,
	}

	function := mtype.method.Func
	var returnValues []reflect.Value

	// Invoke the method, providing a new value for the reply.
	fn := func(ctx context.Context, req server.Request, stream interface{}) error {
		returnValues = function.Call([]reflect.Value{service.rcvr, mtype.prepareContext(ctx), reflect.ValueOf(stream)})
		if err := returnValues[0].Interface(); err != nil {
			return err.(error)
		}

		return nil
	}

	for i := len(opts.HdlrWrappers); i > 0; i-- {
		fn = opts.HdlrWrappers[i-1](fn)
	}

	statusCode := codes.OK
	statusDesc := ""

	appErr := fn(ctx, r, ss)
	if appErr != nil {
		if err, ok := appErr.(*rpcError); ok {
			statusCode = err.code
			statusDesc = err.desc
		} else if err, ok := appErr.(*errors.Error); ok {
			statusCode = microError(err)
			statusDesc = appErr.Error()
		} else {
			statusCode = convertCode(appErr)
			statusDesc = appErr.Error()
		}
	}

	return status.New(statusCode, statusDesc).Err()
}

func (g *grpcServer) newGRPCCodec(contentType string) (encoding.Codec, error) {
	codecs := make(map[string]encoding.Codec)
	if g.opts.Context != nil {
		if v := g.opts.Context.Value(codecsKey{}); v != nil {
			codecs = v.(map[string]encoding.Codec)
		}
	}
	if c, ok := codecs[contentType]; ok {
		return c, nil
	}
	if c, ok := defaultGRPCCodecs[contentType]; ok {
		return c, nil
	}
	return nil, fmt.Errorf("Unsupported Content-Type: %s", contentType)
}

func (g *grpcServer) newCodec(contentType string) (codec.NewCodec, error) {
	if cf, ok := g.opts.Codecs[contentType]; ok {
		return cf, nil
	}
	if cf, ok := defaultRPCCodecs[contentType]; ok {
		return cf, nil
	}
	return nil, fmt.Errorf("Unsupported Content-Type: %s", contentType)
}

func (g *grpcServer) Options() server.Options {
	g.RLock()
	opts := g.opts
	g.RUnlock()

	return opts
}

func (g *grpcServer) Init(opts ...server.Option) error {
	g.configure(opts...)
	return nil
}

func (g *grpcServer) NewHandler(h interface{}, opts ...server.HandlerOption) server.Handler {
	return newRpcHandler(h, opts...)
}

// Handle从参数h中提取服务信息
// 以服务名为key保存在g.rpc.serviceMap中
// 并且以服务名为key保存在g.handlers中
func (g *grpcServer) Handle(h server.Handler) error {
	if err := g.rpc.register(h.Handler()); err != nil {
		return err
	}
	// 服务名 -> 服务handler(例如：rpcHandler)，即一个server可以同时提供多个服务
	g.handlers[h.Name()] = h
	return nil
}

func (g *grpcServer) NewSubscriber(topic string, sb interface{}, opts ...server.SubscriberOption) server.Subscriber {
	return newSubscriber(topic, sb, opts...)
}

// 以sb为key保存到map
func (g *grpcServer) Subscribe(sb server.Subscriber) error {
	sub, ok := sb.(*subscriber)
	if !ok {
		return fmt.Errorf("invalid subscriber: expected *subscriber")
	}
	if len(sub.handlers) == 0 {
		return fmt.Errorf("invalid subscriber: no handler functions")
	}

	if err := validateSubscriber(sb); err != nil {
		return err
	}

	g.Lock()

	_, ok = g.subscribers[sub]
	if ok {
		return fmt.Errorf("subscriber %v already exists", sub)
	}
	g.subscribers[sub] = nil
	g.Unlock()
	return nil
}

func (g *grpcServer) Register() error {
	var err error
	var advt, host, port string

	// parse address for host, port
	config := g.opts

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	if cnt := strings.Count(advt, ":"); cnt >= 1 { // 包含端口
		// ipv6 address in format [host]:port or ipv4 host:port
		host, port, err = net.SplitHostPort(advt)
		if err != nil {
			return err
		}
	} else { // 只有地址无端口
		host = advt
	}

	// 找到本机ip局域网地址
	addr, err := addr.Extract(host)
	if err != nil {
		return err
	}

	// register service
	// 服务节点
	node := &registry.Node{
		Id:       config.Name + "-" + config.Id,
		Address:  mnet.HostPort(addr, port),
		Metadata: config.Metadata,
	}

	node.Metadata["broker"] = config.Broker.String()
	node.Metadata["registry"] = config.Registry.String()
	node.Metadata["server"] = g.String()
	node.Metadata["transport"] = g.String()
	node.Metadata["protocol"] = "grpc"

	g.RLock()
	// Maps are ordered randomly, sort the keys for consistency
	var handlerList []string // 服务名数组[srv1, srv2, ...]
	for n, e := range g.handlers {
		// Only advertise non internal handlers
		if !e.Options().Internal {
			handlerList = append(handlerList, n)
		}
	}
	// 服务名排序
	sort.Strings(handlerList)

	var subscriberList []*subscriber
	for e := range g.subscribers {
		// Only advertise non internal subscribers
		if !e.Options().Internal {
			subscriberList = append(subscriberList, e)
		}
	}
	sort.Slice(subscriberList, func(i, j int) bool {
		return subscriberList[i].topic > subscriberList[j].topic
	})

	endpoints := make([]*registry.Endpoint, 0, len(handlerList)+len(subscriberList))
	for _, n := range handlerList {
		endpoints = append(endpoints, g.handlers[n].Endpoints()...)
	}
	// 每一个最终会被调用的subscriber函数或者receiver方法
	for _, e := range subscriberList {
		endpoints = append(endpoints, e.Endpoints()...)
	}
	g.RUnlock()

	service := &registry.Service{
		Name:      config.Name,
		Version:   config.Version,
		Nodes:     []*registry.Node{node},
		Endpoints: endpoints,
	}

	g.Lock()
	registered := g.registered
	g.Unlock()

	if !registered {
		log.Logf("Registry [%s] Registering node: %s", config.Registry.String(), node.Id)
	}

	// create registry options
	rOpts := []registry.RegisterOption{registry.RegisterTTL(config.RegisterTTL)}

	// 调用注册接口进行注册
	if err := config.Registry.Register(service, rOpts...); err != nil {
		return err
	}

	// already registered? don't need to register subscribers
	if registered {
		return nil
	}

	g.Lock()
	defer g.Unlock()

	g.registered = true

	for sb := range g.subscribers {
		// 构造一个handler，注册到broker，当消息到达时会调用该handelr。
		handler := g.createSubHandler(sb, g.opts)
		var opts []broker.SubscribeOption
		if queue := sb.Options().Queue; len(queue) > 0 {
			opts = append(opts, broker.Queue(queue))
		}

		if !sb.Options().AutoAck {
			opts = append(opts, broker.DisableAutoAck())
		}

		// 注册到broker服务的内存映射
		// 将topic作为服务注册到registry
		sub, err := config.Broker.Subscribe(sb.Topic(), handler, opts...)
		if err != nil {
			return err
		}
		g.subscribers[sb] = []broker.Subscriber{sub}
	}

	return nil
}

func (g *grpcServer) Deregister() error {
	var err error
	var advt, host, port string

	config := g.opts

	// check the advertise address first
	// if it exists then use it, otherwise
	// use the address
	if len(config.Advertise) > 0 {
		advt = config.Advertise
	} else {
		advt = config.Address
	}

	if cnt := strings.Count(advt, ":"); cnt >= 1 {
		// ipv6 address in format [host]:port or ipv4 host:port
		host, port, err = net.SplitHostPort(advt)
		if err != nil {
			return err
		}
	} else {
		host = advt
	}

	addr, err := addr.Extract(host)
	if err != nil {
		return err
	}

	node := &registry.Node{
		Id:      config.Name + "-" + config.Id,
		Address: mnet.HostPort(addr, port),
	}

	service := &registry.Service{
		Name:    config.Name,
		Version: config.Version,
		Nodes:   []*registry.Node{node},
	}

	log.Logf("Deregistering node: %s", node.Id)
	if err := config.Registry.Deregister(service); err != nil {
		return err
	}

	g.Lock()

	if !g.registered {
		g.Unlock()
		return nil
	}

	g.registered = false

	for sb, subs := range g.subscribers {
		for _, sub := range subs {
			log.Logf("Unsubscribing from topic: %s", sub.Topic())
			sub.Unsubscribe()
		}
		g.subscribers[sb] = nil
	}

	g.Unlock()
	return nil
}

func (g *grpcServer) Start() error {
	// 加锁，判断是否已经启动
	g.RLock()
	if g.started {
		g.RUnlock()
		return nil
	}
	g.RUnlock()

	config := g.Options()

	// micro: config.Transport.Listen(config.Address)
	// 如果未配置端口会随机分配端口
	ts, err := net.Listen("tcp", config.Address)
	if err != nil {
		return err
	}

	log.Logf("Server [grpc] Listening on %s", ts.Addr().String())
	g.Lock()
	// 将地址更新成实际使用的地址，例如随机分配的新端口
	g.opts.Address = ts.Addr().String()
	g.Unlock()

	// connect to the broker
	// 如果是http broker则启动broker的http server
	// 如果grpc broker则启动grpc server
	if err := config.Broker.Connect(); err != nil {
		return err
	}

	baddr := config.Broker.Address()
	bname := config.Broker.String()

	log.Logf("Broker [%s] Connected to %s", bname, baddr)

	// announce self to the world
	// 注册到服务发现
	if err := g.Register(); err != nil {
		log.Log("Server register error: ", err)
	}

	// micro: go ts.Accept(s.accept)
	go func() {
		if err := g.srv.Serve(ts); err != nil {
			log.Log("gRPC Server start error: ", err)
		}
	}()

	go func() {
		t := new(time.Ticker)

		// only process if it exists
		if g.opts.RegisterInterval > time.Duration(0) {
			// new ticker
			t = time.NewTicker(g.opts.RegisterInterval)
		}

		// return error chan
		var ch chan error

	Loop:
		for {
			select {
			// register self on interval
			case <-t.C:
				if err := g.Register(); err != nil {
					log.Log("Server register error: ", err)
				}
			// wait for exit
			case ch = <-g.exit:
				break Loop
			}
		}

		// deregister self
		if err := g.Deregister(); err != nil {
			log.Log("Server deregister error: ", err)
		}

		// wait for waitgroup
		if g.wg != nil {
			g.wg.Wait()
		}

		// stop the grpc server
		g.srv.GracefulStop()

		// close transport
		ch <- nil

		// disconnect broker
		config.Broker.Disconnect()
	}()

	// mark the server as started
	g.Lock()
	g.started = true
	g.Unlock()

	return nil
}

func (g *grpcServer) Stop() error {
	g.RLock()
	if !g.started {
		g.RUnlock()
		return nil
	}
	g.RUnlock()

	ch := make(chan error)
	g.exit <- ch

	var err error
	select {
	case err = <-ch:
		g.Lock()
		g.started = false
		g.Unlock()
	}

	return err
}

func (g *grpcServer) String() string {
	return "grpc"
}

func NewServer(opts ...server.Option) server.Server {
	return newGRPCServer(opts...)
}
