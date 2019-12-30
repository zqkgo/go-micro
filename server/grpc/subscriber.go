package grpc

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/errors"
	"github.com/micro/go-micro/metadata"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/server"
	"github.com/micro/go-micro/util/log"
)

const (
	subSig = "func(context.Context, interface{}) error"
)

// 处理消息的handler
// 可能是一个函数，也可能是receiver的一个方法
type handler struct {
	// 订阅者最终要执行的方法
	// 如果是函数则调用时正常入参
	// 如果是receiver的方法，则调用时按Method.Func的规则
	method  reflect.Value
	reqType reflect.Type
	ctxType reflect.Type
}

type subscriber struct {
	topic string
	// 订阅者值，如果subscriber是一个receiver，那么基于反射的handler调用需要rcvr作为第一个参数
	rcvr reflect.Value
	// 订阅者类型，可能是一个函数或带方法集的receiver
	typ reflect.Type
	// 函数或者多个方法的receiver
	subscriber interface{}
	// 如果subscriber是一个函数则只有一个handler
	// 如果subscriber是一个receiver有可能有多个handler
	// 接收到event后会逐个调用每个handler的method
	handlers []*handler
	// 根据subscriber函数或receiver的多个方法构造
	// 与正常的服务方法一起注册到registry
	endpoints []*registry.Endpoint
	opts      server.SubscriberOptions
}

func newSubscriber(topic string, sub interface{}, opts ...server.SubscriberOption) server.Subscriber {
	options := server.SubscriberOptions{
		AutoAck: true,
	}

	for _, o := range opts {
		o(&options)
	}

	var endpoints []*registry.Endpoint
	var handlers []*handler

	if typ := reflect.TypeOf(sub); typ.Kind() == reflect.Func { // 如果订阅者是函数
		h := &handler{
			method: reflect.ValueOf(sub),
		}

		// 函数的入参有1个还是2个
		switch typ.NumIn() {
		case 1:
			h.reqType = typ.In(0)
		case 2:
			h.ctxType = typ.In(0)
			h.reqType = typ.In(1)
		}

		handlers = append(handlers, h)

		endpoints = append(endpoints, &registry.Endpoint{
			Name:    "Func",
			Request: extractSubValue(typ),
			Metadata: map[string]string{
				"topic":      topic,
				"subscriber": "true",
			},
		})
	} else { // 如果订阅者是带方法集的receiver
		hdlr := reflect.ValueOf(sub)
		name := reflect.Indirect(hdlr).Type().Name()
		// receiver的每一个方法
		for m := 0; m < typ.NumMethod(); m++ {
			method := typ.Method(m)
			h := &handler{
				method: method.Func,
			}

			switch method.Type.NumIn() {
			case 2:
				h.reqType = method.Type.In(1)
			case 3:
				h.ctxType = method.Type.In(1)
				h.reqType = method.Type.In(2)
			}

			handlers = append(handlers, h)

			endpoints = append(endpoints, &registry.Endpoint{
				Name:    name + "." + method.Name,
				Request: extractSubValue(method.Type),
				Metadata: map[string]string{
					"topic":      topic,
					"subscriber": "true",
				},
			})
		}
	}

	return &subscriber{
		rcvr:       reflect.ValueOf(sub),
		typ:        reflect.TypeOf(sub),
		topic:      topic,
		subscriber: sub,
		handlers:   handlers,
		endpoints:  endpoints,
		opts:       options,
	}
}

func validateSubscriber(sub server.Subscriber) error {
	typ := reflect.TypeOf(sub.Subscriber())
	var argType reflect.Type

	if typ.Kind() == reflect.Func {
		name := "Func"
		switch typ.NumIn() {
		case 2:
			argType = typ.In(1)
		default:
			return fmt.Errorf("subscriber %v takes wrong number of args: %v required signature %s", name, typ.NumIn(), subSig)
		}
		if !isExportedOrBuiltinType(argType) {
			return fmt.Errorf("subscriber %v argument type not exported: %v", name, argType)
		}
		if typ.NumOut() != 1 {
			return fmt.Errorf("subscriber %v has wrong number of outs: %v require signature %s",
				name, typ.NumOut(), subSig)
		}
		if returnType := typ.Out(0); returnType != typeOfError {
			return fmt.Errorf("subscriber %v returns %v not error", name, returnType.String())
		}
	} else {
		hdlr := reflect.ValueOf(sub.Subscriber())
		name := reflect.Indirect(hdlr).Type().Name()

		for m := 0; m < typ.NumMethod(); m++ {
			method := typ.Method(m)

			switch method.Type.NumIn() {
			case 3:
				argType = method.Type.In(2)
			default:
				return fmt.Errorf("subscriber %v.%v takes wrong number of args: %v required signature %s",
					name, method.Name, method.Type.NumIn(), subSig)
			}

			if !isExportedOrBuiltinType(argType) {
				return fmt.Errorf("%v argument type not exported: %v", name, argType)
			}
			if method.Type.NumOut() != 1 {
				return fmt.Errorf(
					"subscriber %v.%v has wrong number of outs: %v require signature %s",
					name, method.Name, method.Type.NumOut(), subSig)
			}
			if returnType := method.Type.Out(0); returnType != typeOfError {
				return fmt.Errorf("subscriber %v.%v returns %v not error", name, method.Name, returnType.String())
			}
		}
	}

	return nil
}

// 构造一个handler，注册到broker，当消息到达时会调用该handelr。
func (g *grpcServer) createSubHandler(sb *subscriber, opts server.Options) broker.Handler {
	return func(p broker.Event) (err error) {

		defer func() {
			if r := recover(); r != nil {
				log.Log("panic recovered: ", r)
				log.Logf(string(debug.Stack()))
				err = errors.InternalServerError("go.micro.server", "panic recovered: %v", r)
			}
		}()

		msg := p.Message()
		ct := msg.Header["Content-Type"]
		if len(ct) == 0 {
			msg.Header["Content-Type"] = defaultContentType
			ct = defaultContentType
		}
		// 获取编解码对象
		cf, err := g.newGRPCCodec(ct)
		if err != nil {
			return err
		}

		// 复制一份消息头
		hdr := make(map[string]string)
		for k, v := range msg.Header {
			hdr[k] = v
		}
		delete(hdr, "Content-Type")
		// 将消息头作为元信息保存到context
		ctx := metadata.NewContext(context.Background(), hdr)

		results := make(chan error, len(sb.handlers))

		for i := 0; i < len(sb.handlers); i++ {
			handler := sb.handlers[i]

			var isVal bool
			var req reflect.Value

			// 创建一个入参类型的指针
			if handler.reqType.Kind() == reflect.Ptr {
				req = reflect.New(handler.reqType.Elem())
			} else {
				req = reflect.New(handler.reqType)
				isVal = true
			}
			if isVal {
				req = req.Elem()
			}

			// 将消息体解码到入参
			if err = cf.Unmarshal(msg.Body, req.Interface()); err != nil {
				return err
			}

			fn := func(ctx context.Context, msg server.Message) error {
				var vals []reflect.Value
				// subscriber是带方法集的receiver
				if sb.typ.Kind() != reflect.Func {
					vals = append(vals, sb.rcvr)
				}
				if handler.ctxType != nil {
					vals = append(vals, reflect.ValueOf(ctx))
				}

				vals = append(vals, reflect.ValueOf(msg.Payload()))

				// 最终调用handler
				returnValues := handler.method.Call(vals)
				if rerr := returnValues[0].Interface(); rerr != nil {
					return rerr.(error)
				}
				return nil
			}

			// 执行封装函数
			for i := len(opts.SubWrappers); i > 0; i-- {
				fn = opts.SubWrappers[i-1](fn)
			}

			if g.wg != nil {
				g.wg.Add(1)
			}
			// 异步调用
			go func() {
				if g.wg != nil {
					defer g.wg.Done()
				}
				results <- fn(ctx, &rpcMessage{
					topic:       sb.topic,
					contentType: ct,
					payload:     req.Interface(),
					header:      msg.Header,
					body:        msg.Body,
				})
			}()
		}
		// 收集subscribers的每个handler的执行错误结果
		var errors []string
		for i := 0; i < len(sb.handlers); i++ {
			if rerr := <-results; err != nil {
				errors = append(errors, rerr.Error())
			}
		}
		if len(errors) > 0 {
			err = fmt.Errorf("subscriber error: %s", strings.Join(errors, "\n"))
		}

		return err
	}
}

func (s *subscriber) Topic() string {
	return s.topic
}

func (s *subscriber) Subscriber() interface{} {
	return s.subscriber
}

func (s *subscriber) Endpoints() []*registry.Endpoint {
	return s.endpoints
}

func (s *subscriber) Options() server.SubscriberOptions {
	return s.opts
}
