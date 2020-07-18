package kinglink

import (
	"context"
	"sync"
)

type InterceptorFunc func(ctx context.Context, typeName string, args map[string]interface{}, opts *Options) (string, map[string]interface{}, error)

var (
	interceptorsLock sync.Mutex
	interceptors     = map[string]InterceptorFunc{}
)

func AddInterceptor(name string, a InterceptorFunc) {
	interceptorsLock.Lock()
	defer interceptorsLock.Unlock()

	interceptors[name] = a
}

func DefaultInterceptor() InterceptorFunc {
	return InterceptorFunc(func(ctx context.Context, typeName string, args map[string]interface{}, opts *Options) (string, map[string]interface{}, error) {

		interceptorsLock.Lock()
		interceptor := interceptors[typeName]
		interceptorsLock.Unlock()

		if interceptor != nil {
			return interceptor(ctx, typeName, args, opts)
		}
		return typeName, args, nil
	})
}
