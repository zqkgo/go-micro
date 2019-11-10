package server

import (
	"reflect"

	"github.com/micro/go-micro/registry"
)

type rpcHandler struct {
	// 服务名
	name string
	// 方法的receiver
	handler interface{}
	// 用于注册到registry
	endpoints []*registry.Endpoint
	opts      HandlerOptions
}

func newRpcHandler(handler interface{}, opts ...HandlerOption) Handler {
	options := HandlerOptions{
		Metadata: make(map[string]map[string]string),
	}

	for _, o := range opts {
		o(&options)
	}

	typ := reflect.TypeOf(handler)
	hdlr := reflect.ValueOf(handler)
	// hdlr可能是指针或值，这里必然是指针
	name := reflect.Indirect(hdlr).Type().Name()

	var endpoints []*registry.Endpoint

	// 遍历服务的每个方法构造可用于服务注册的对象
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
		name:      name,
		handler:   handler,
		endpoints: endpoints,
		opts:      options,
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

func (r *rpcHandler) Options() HandlerOptions {
	return r.opts
}
