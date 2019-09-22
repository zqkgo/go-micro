// Package metadata is a way of defining message headers
package metadata

import (
	"context"
)

type metaKey struct{}

// Metadata is our way of representing request headers internally.
// They're used at the RPC level and translate back and forth
// from Transport headers.
// 存储头信息的对象
type Metadata map[string]string

// 复制头信息，因为是map类型本质是指向hmap的指针
// 所以需要循环遍历每一个元素完成复制
func Copy(md Metadata) Metadata {
	cmd := make(Metadata)
	for k, v := range md {
		cmd[k] = v
	}
	return cmd
}

// 从一个ctx中提取头信息
func FromContext(ctx context.Context) (Metadata, bool) {
	md, ok := ctx.Value(metaKey{}).(Metadata)
	return md, ok
}

// 从一个ctx复制出一个新的ctx，并且将头信息保存在该ctx中，key需是自定义类型
func NewContext(ctx context.Context, md Metadata) context.Context {
	return context.WithValue(ctx, metaKey{}, md)
}
