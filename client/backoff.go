package client

import (
	"context"
	"math"
	"time"
)

// 每次失败，则调用符合此签名的函数，决定下次要等多久后重试
type BackoffFunc func(ctx context.Context, req Request, attempts int) (time.Duration, error)

// exponential backoff
// 失败则延长sleep时间
func exponentialBackoff(ctx context.Context, req Request, attempts int) (time.Duration, error) {
	if attempts == 0 {
		return time.Duration(0), nil
	}
	return time.Duration(math.Pow(10, float64(attempts))) * time.Millisecond, nil
}
