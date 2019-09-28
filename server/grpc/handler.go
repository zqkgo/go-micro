package grpc

import (
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/server"
	"reflect"
)

type rpcHandler struct {
	name    string
	handler interface{}
	// 服务的方法集合
	endpoints []*registry.Endpoint
	opts      server.HandlerOptions
}

// 构造提供服务的对象
func newRpcHandler(handler interface{}, opts ...server.HandlerOption) server.Handler {
	options := server.HandlerOptions{
		Metadata: make(map[string]map[string]string),
	}
	// 调用每个可以修改options的函数
	for _, o := range opts {
		o(&options)
	}
	// 取出实现接口的类型信息
	typ := reflect.TypeOf(handler)
	// 取出实现接口的值信息，hdlr是指针类型
	hdlr := reflect.ValueOf(handler)
	// 指针类型指向的类型名称
	name := reflect.Indirect(hdlr).Type().Name()

	var endpoints []*registry.Endpoint
	// 提取出每一个方法，构造表示方法的Endpoint对象数组
	for m := 0; m < typ.NumMethod(); m++ {
		if e := extractEndpoint(typ.Method(m)); e != nil {
			e.Name = name + "." + e.Name

			for k, v := range options.Metadata[e.Name] {
				e.Metadata[k] = v
			}

			endpoints = append(endpoints, e)
		}
	}

	return &rpcHandler{
		name:      name,      // 服务名
		handler:   handler,   // 提供服务的handler
		endpoints: endpoints, // 服务具体执行的方法数组
		opts:      options,   // 一些配置项
	}
}

func (r *rpcHandler) Name() string {
	return r.name
}

func (r *rpcHandler) Handler() interface{} {
	return r.handler
}

func (r *rpcHandler) Endpoints() []*registry.Endpoint {
	return r.endpoints
}

func (r *rpcHandler) Options() server.HandlerOptions {
	return r.opts
}
