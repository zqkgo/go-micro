// Package selector is a way to pick a list of service nodes
package selector

import (
	"errors"

	"github.com/micro/go-micro/registry"
)

// Selector builds on the registry as a mechanism to pick nodes
// and mark their status. This allows host pools and other things
// to be built using various algorithms.
type Selector interface {
	Init(opts ...Option) error
	Options() Options
	// Select returns a function which should return the next node
	Select(service string, opts ...SelectOption) (Next, error)
	// Mark sets the success/error against a node
	Mark(service string, node *registry.Node, err error)
	// Reset returns state back to zero for a service
	Reset(service string)
	// Close renders the selector unusable
	Close() error
	// Name of the selector
	String() string
}

// Next is a function that returns the next node
// based on the selector's strategy
type Next func() (*registry.Node, error)

// Filter is used to filter a service during the selection process
// 获取到服务对象之后通过Filter对之进行过滤
type Filter func([]*registry.Service) []*registry.Service

// Strategy is a selection strategy e.g random, round robin
// Strategy是一种函数类型，符合此签名的函数，接收一个已经服务发现的对象
// 返回的Next也是一种函数类型，Next是真正 执行算法，返回节点 的函数
type Strategy func([]*registry.Service) Next

var (
	DefaultSelector = NewSelector()

	ErrNotFound      = errors.New("not found")
	ErrNoneAvailable = errors.New("none available")
)
