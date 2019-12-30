package client

import (
	"context"
	"time"

	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/client/selector"
	"github.com/micro/go-micro/codec"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/transport"
)

type Options struct {
	// Used to select codec
	// 用来选择编码方式
	ContentType string

	// Plugged interfaces
	Broker    broker.Broker
	Codecs    map[string]codec.NewCodec
	Registry  registry.Registry
	Selector  selector.Selector
	Transport transport.Transport

	// Router sets the router
	Router Router

	// Connection Pool
	// 连接池大小
	PoolSize int
	// 连接有效期，连接存在时间如果超过这个值，会被连接池关闭
	PoolTTL time.Duration

	// Middleware for client
	// 设置入栈，调用出栈，创建完grpcClient只有调用
	Wrappers []Wrapper

	// Default Call Options
	CallOptions CallOptions

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type CallOptions struct {
	// 可修改*SelectionOptions的函数数组
	SelectOptions []selector.SelectOption

	// Address of remote hosts
	// 在grpc调用中只选第一个地址作为代理地址来代替服务发现
	// 在rpc调用中，每个地址表示一个节点地址
	Address []string
	// Backoff func
	// 兜底函数，获取重试等待时间
	Backoff BackoffFunc
	// Check if retriable func
	// 调用此方法判断是否继续重试
	Retry RetryFunc
	// Transport Dial Timeout
	// 与grpc server建立tcp连接时的超时时间
	DialTimeout time.Duration
	// Number of Call attempts
	// 请求重试次数，默认1次
	Retries int
	// Request/Response timeout
	// 超时时间，默认5s
	RequestTimeout time.Duration

	// Middleware for low level call func
	CallWrappers []CallWrapper

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type PublishOptions struct {
	// Exchange is the routing exchange for the message
	Exchange string
	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

type MessageOptions struct {
	ContentType string
}

type RequestOptions struct {
	ContentType string
	Stream      bool

	// Other options for implementations of the interface
	// can be stored in a context
	Context context.Context
}

func NewOptions(options ...Option) Options {
	opts := Options{
		Context:     context.Background(),
		ContentType: DefaultContentType,
		Codecs:      make(map[string]codec.NewCodec),
		CallOptions: CallOptions{
			Backoff:        DefaultBackoff,
			Retry:          DefaultRetry,
			Retries:        DefaultRetries,
			RequestTimeout: DefaultRequestTimeout,
			DialTimeout:    transport.DefaultDialTimeout,
		},
		PoolSize:  DefaultPoolSize,
		PoolTTL:   DefaultPoolTTL,
		Broker:    broker.DefaultBroker,
		Selector:  selector.DefaultSelector,
		Registry:  registry.DefaultRegistry,
		Transport: transport.DefaultTransport,
	}

	for _, o := range options {
		o(&opts)
	}

	return opts
}

// Broker to be used for pub/sub
func Broker(b broker.Broker) Option {
	return func(o *Options) {
		o.Broker = b
	}
}

// Codec to be used to encode/decode requests for a given content type
func Codec(contentType string, c codec.NewCodec) Option {
	return func(o *Options) {
		o.Codecs[contentType] = c
	}
}

// Default content type of the client
func ContentType(ct string) Option {
	return func(o *Options) {
		o.ContentType = ct
	}
}

// PoolSize sets the connection pool size
func PoolSize(d int) Option {
	return func(o *Options) {
		o.PoolSize = d
	}
}

// PoolTTL sets the connection pool ttl
func PoolTTL(d time.Duration) Option {
	return func(o *Options) {
		o.PoolTTL = d
	}
}

// Registry to find nodes for a given service
func Registry(r registry.Registry) Option {
	return func(o *Options) {
		o.Registry = r
		// set in the selector
		o.Selector.Init(selector.Registry(r))
	}
}

// Transport to use for communication e.g http, rabbitmq, etc
func Transport(t transport.Transport) Option {
	return func(o *Options) {
		o.Transport = t
	}
}

// Select is used to select a node to route a request to
func Selector(s selector.Selector) Option {
	return func(o *Options) {
		o.Selector = s
	}
}

// Adds a Wrapper to a list of options passed into the client
// 入栈
func Wrap(w Wrapper) Option {
	return func(o *Options) {
		o.Wrappers = append(o.Wrappers, w)
	}
}

// Adds a Wrapper to the list of CallFunc wrappers
func WrapCall(cw ...CallWrapper) Option {
	return func(o *Options) {
		o.CallOptions.CallWrappers = append(o.CallOptions.CallWrappers, cw...)
	}
}

// Backoff is used to set the backoff function used
// when retrying Calls
func Backoff(fn BackoffFunc) Option {
	return func(o *Options) {
		o.CallOptions.Backoff = fn
	}
}

// Number of retries when making the request.
// Should this be a Call Option?
func Retries(i int) Option {
	return func(o *Options) {
		o.CallOptions.Retries = i
	}
}

// Retry sets the retry function to be used when re-trying.
func Retry(fn RetryFunc) Option {
	return func(o *Options) {
		o.CallOptions.Retry = fn
	}
}

// The request timeout.
// Should this be a Call Option?
func RequestTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.CallOptions.RequestTimeout = d
	}
}

// Transport dial timeout
func DialTimeout(d time.Duration) Option {
	return func(o *Options) {
		o.CallOptions.DialTimeout = d
	}
}

// Call Options

// WithExchange sets the exchange to route a message through
func WithExchange(e string) PublishOption {
	return func(o *PublishOptions) {
		o.Exchange = e
	}
}

// WithAddress sets the remote addresses to use rather than using service discovery
func WithAddress(a ...string) CallOption {
	return func(o *CallOptions) {
		o.Address = a
	}
}

// 构造client时可以使用此函数设置selector的策略、过滤器等
// client会在选择节点的时候应用此配置项
func WithSelectOption(so ...selector.SelectOption) CallOption {
	return func(o *CallOptions) {
		o.SelectOptions = append(o.SelectOptions, so...)
	}
}

// WithCallWrapper is a CallOption which adds to the existing CallFunc wrappers
func WithCallWrapper(cw ...CallWrapper) CallOption {
	return func(o *CallOptions) {
		o.CallWrappers = append(o.CallWrappers, cw...)
	}
}

// WithBackoff is a CallOption which overrides that which
// set in Options.CallOptions
func WithBackoff(fn BackoffFunc) CallOption {
	return func(o *CallOptions) {
		o.Backoff = fn
	}
}

// WithRetry is a CallOption which overrides that which
// set in Options.CallOptions
func WithRetry(fn RetryFunc) CallOption {
	return func(o *CallOptions) {
		o.Retry = fn
	}
}

// WithRetries is a CallOption which overrides that which
// set in Options.CallOptions
func WithRetries(i int) CallOption {
	return func(o *CallOptions) {
		o.Retries = i
	}
}

// WithRequestTimeout is a CallOption which overrides that which
// set in Options.CallOptions
func WithRequestTimeout(d time.Duration) CallOption {
	return func(o *CallOptions) {
		o.RequestTimeout = d
	}
}

// WithDialTimeout is a CallOption which overrides that which
// set in Options.CallOptions
func WithDialTimeout(d time.Duration) CallOption {
	return func(o *CallOptions) {
		o.DialTimeout = d
	}
}

// Request Options

func WithContentType(ct string) RequestOption {
	return func(o *RequestOptions) {
		o.ContentType = ct
	}
}

func StreamingRequest() RequestOption {
	return func(o *RequestOptions) {
		o.Stream = true
	}
}

// WithRouter sets the client router
func WithRouter(r Router) Option {
	return func(o *Options) {
		o.Router = r
	}
}
