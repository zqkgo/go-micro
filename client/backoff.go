package client

import (
	"context"
	"math"
	"time"
)

type BackoffFunc func(ctx context.Context, req Request, attempts int) (time.Duration, error)

// exponential backoff
// 失败则延长sleep时间
func exponentialBackoff(ctx context.Context, req Request, attempts int) (time.Duration, error) {
	if attempts == 0 {
		return time.Duration(0), nil
	}
	return time.Duration(math.Pow(10, float64(attempts))) * time.Millisecond, nil
}
