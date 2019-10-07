package grpc

import (
	"fmt"
	"strings"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/codec"
)

// 请求对象包含了如下信息
// 1. 哪个服务
// 2. 哪个endpoint
// 3. 编码方式
// 4. 配置项
type grpcRequest struct {
	// Service名称，例如go.micro.srv.save
	service     string
	// service.method例如Save.Categories
	method      string
	// 内容类型（格式），例如application/grpc+proto
	// 用来选择codec编码方式
	contentType string
	// 请求对象message，例如*CategoriesReq
	request     interface{}
	opts        client.RequestOptions
	codec       codec.Codec
}

// service Struct.Method /service.Struct/Method
func methodToGRPC(service, method string) string {
	// no method or already grpc method
	if len(method) == 0 || method[0] == '/' {
		return method
	}

	// assume method is Foo.Bar
	mParts := strings.Split(method, ".")
	if len(mParts) != 2 {
		return method
	}

	if len(service) == 0 {
		return fmt.Sprintf("/%s/%s", mParts[0], mParts[1])
	}

	// return /pkg.Foo/Bar
	return fmt.Sprintf("/%s.%s/%s", service, mParts[0], mParts[1])
}

func newGRPCRequest(service, method string, request interface{}, contentType string, reqOpts ...client.RequestOption) client.Request {
	var opts client.RequestOptions
	for _, o := range reqOpts {
		o(&opts)
	}

	// set the content-type specified
	// 如果参数配置了，则不使用GRPC客户端的全局contentType
	if len(opts.ContentType) > 0 {
		contentType = opts.ContentType
	}

	return &grpcRequest{
		service:     service,
		method:      method,
		request:     request,
		contentType: contentType,
		opts:        opts,
	}
}

func (g *grpcRequest) ContentType() string {
	return g.contentType
}

func (g *grpcRequest) Service() string {
	return g.service
}

func (g *grpcRequest) Method() string {
	return g.method
}

func (g *grpcRequest) Endpoint() string {
	return g.method
}

func (g *grpcRequest) Codec() codec.Writer {
	return g.codec
}

func (g *grpcRequest) Body() interface{} {
	return g.request
}

func (g *grpcRequest) Stream() bool {
	return g.opts.Stream
}
