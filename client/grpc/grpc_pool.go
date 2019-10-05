package grpc

import (
	"sync"
	"time"

	"google.golang.org/grpc"
)

type pool struct {
	// 连接池大小，超过此值则不会加入新连接
	size int
	// 连接有效期，超过此值的连接会被关闭
	ttl  int64

	sync.Mutex
	// 节点IP地址到一组连接的映射
	conns map[string][]*poolConn
}

type poolConn struct {
	*grpc.ClientConn
	// 连接创建时的时间戳
	created int64
}

func newPool(size int, ttl time.Duration) *pool {
	return &pool{
		size:  size,
		ttl:   int64(ttl.Seconds()),
		conns: make(map[string][]*poolConn),
	}
}

func (p *pool) getConn(addr string, opts ...grpc.DialOption) (*poolConn, error) {
	p.Lock()
	conns := p.conns[addr]
	now := time.Now().Unix()

	// while we have conns check age and then return one
	// otherwise we'll create a new conn
	for len(conns) > 0 {
		// 出栈一个连接
		conn := conns[len(conns)-1]
		conns = conns[:len(conns)-1]
		p.conns[addr] = conns

		// if conn is old kill it and move on
		// 连接超时则关闭
		if d := now - conn.created; d > p.ttl {
			conn.ClientConn.Close()
			continue
		}

		// we got a good conn, lets unlock and return it
		p.Unlock()

		return conn, nil
	}

	p.Unlock()

	// create new conn
	// 栈空，新建连接
	cc, err := grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &poolConn{cc, time.Now().Unix()}, nil
}

func (p *pool) release(addr string, conn *poolConn, err error) {
	// don't store the conn if it has errored
	// grpc调用出错则关闭连接
	if err != nil {
		conn.ClientConn.Close()
		return
	}

	// otherwise put it back for reuse
	// 栈满则关闭连接，返回。否则，连接入栈。
	p.Lock()
	conns := p.conns[addr]
	if len(conns) >= p.size {
		p.Unlock()
		conn.ClientConn.Close()
		return
	}
	p.conns[addr] = append(conns, conn)
	p.Unlock()
}
