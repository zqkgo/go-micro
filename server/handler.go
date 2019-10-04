package server

import "context"

type HandlerOption func(*HandlerOptions)

// 实现服务端接口的对象(Handler)持有的选项
type HandlerOptions struct {
	// 向服务发现注册时，用来控制是否注册，true-内部使用-否 false-非内部使用-是
	Internal bool
	// 存储服务所有的方法的头信息
	// 服务名.方法名 -> 头信息，例如：Save.Categories => 头信息
	Metadata map[string]map[string]string
}

type SubscriberOption func(*SubscriberOptions)

type SubscriberOptions struct {
	// AutoAck defaults to true. When a handler returns
	// with a nil error the message is acked.
	AutoAck  bool
	Queue    string
	Internal bool
	Context  context.Context
}

// EndpointMetadata is a Handler option that allows metadata to be added to
// individual endpoints.
func EndpointMetadata(name string, md map[string]string) HandlerOption {
	return func(o *HandlerOptions) {
		o.Metadata[name] = md
	}
}

// Internal Handler options specifies that a handler is not advertised
// to the discovery system. In the future this may also limit request
// to the internal network or authorised user.
func InternalHandler(b bool) HandlerOption {
	return func(o *HandlerOptions) {
		o.Internal = b
	}
}

// Internal Subscriber options specifies that a subscriber is not advertised
// to the discovery system.
func InternalSubscriber(b bool) SubscriberOption {
	return func(o *SubscriberOptions) {
		o.Internal = b
	}
}
func NewSubscriberOptions(opts ...SubscriberOption) SubscriberOptions {
	opt := SubscriberOptions{
		AutoAck: true,
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}

// DisableAutoAck will disable auto acking of messages
// after they have been handled.
func DisableAutoAck() SubscriberOption {
	return func(o *SubscriberOptions) {
		o.AutoAck = false
	}
}

// Shared queue name distributed messages across subscribers
func SubscriberQueue(n string) SubscriberOption {
	return func(o *SubscriberOptions) {
		o.Queue = n
	}
}

// SubscriberContext set context options to allow broker SubscriberOption passed
func SubscriberContext(ctx context.Context) SubscriberOption {
	return func(o *SubscriberOptions) {
		o.Context = ctx
	}
}
