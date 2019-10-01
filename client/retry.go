package client

import (
	"context"

	"github.com/micro/go-micro/errors"
)

// note that returning either false or a non-nil error will result in the call not being retried
// 判断是否应该重试，false和错误都不会重试
type RetryFunc func(ctx context.Context, req Request, retryCount int, err error) (bool, error)

// RetryAlways always retry on error
func RetryAlways(ctx context.Context, req Request, retryCount int, err error) (bool, error) {
	return true, nil
}

// RetryOnError retries a request on a 500 or timeout error
func RetryOnError(ctx context.Context, req Request, retryCount int, err error) (bool, error) {
	if err == nil {
		return false, nil
	}

	e := errors.Parse(err.Error())
	if e == nil {
		return false, nil
	}

	switch e.Code {
	// retry on timeout or internal server error
	case 408, 500:
		return true, nil
	default:
		return false, nil
	}
}
