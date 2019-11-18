package wrapper

import (
	"context"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
)

// 包含服务客户端和头信息的包裹对象
type clientWrapper struct {
	// 可以向服务发起请求的客户端
	client.Client
	// 头信息
	headers metadata.Metadata
}

var (
	HeaderPrefix = "Micro-"
)

// 创建一个包含头信息的新ctx
// 1. 提取ctx中旧的头信息
// 2. 复制旧的头信息，整合现有头信息
// 3. 复制一个新的ctx，包含整合后的头信息
func (c *clientWrapper) setHeaders(ctx context.Context) context.Context {
	// copy metadata
	mda, _ := metadata.FromContext(ctx)
	md := metadata.Copy(mda)

	// set headers
	for k, v := range c.headers {
		if _, ok := md[k]; !ok {
			md[k] = v
		}
	}

	return metadata.NewContext(ctx, md)
}

func (c *clientWrapper) Call(ctx context.Context, req client.Request, rsp interface{}, opts ...client.CallOption) error {
	ctx = c.setHeaders(ctx)
	return c.Client.Call(ctx, req, rsp, opts...)
}

func (c *clientWrapper) Stream(ctx context.Context, req client.Request, opts ...client.CallOption) (client.Stream, error) {
	ctx = c.setHeaders(ctx)
	return c.Client.Stream(ctx, req, opts...)
}

func (c *clientWrapper) Publish(ctx context.Context, p client.Message, opts ...client.PublishOption) error {
	ctx = c.setHeaders(ctx)
	return c.Client.Publish(ctx, p, opts...)
}

// FromService wraps a client to inject From-Service header into metadata
func FromService(name string, c client.Client) client.Client {
	// 包含服务客户端和头信息的包裹对象
	return &clientWrapper{
		c,
		metadata.Metadata{
			HeaderPrefix + "From-Service": name,
		},
	}
}
