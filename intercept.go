package kinglink

import "context"

type InterceptorFunc func(ctx context.Context, typeName string, args map[string]interface{}, opts *Options) (string, map[string]interface{}, error)
