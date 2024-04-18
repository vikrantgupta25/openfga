// Package registry provides a middleware registration mechanism.
package registry

import (
	"sync"

	"google.golang.org/grpc"
)

var (
	middlewareMu     sync.Mutex
	unaryMiddlewares []grpc.UnaryServerInterceptor
)

// RegisterUnaryServerInterceptors registers the provided interceptors in the order
// in which they are given.
//
// All invocations of RegisterUnaryServerInterceptors register middleware in succession. The
// first chain of middlewares registered will run before the second chain of middlewares etc..
func RegisterUnaryServerInterceptors(
	interceptors ...grpc.UnaryServerInterceptor,
) {
	middlewareMu.Lock()
	defer middlewareMu.Unlock()
	unaryMiddlewares = append(unaryMiddlewares, interceptors...)
}

// RegisteredUnaryServerInterceptors returns the chain of intercptor middlewares registered
// in the middleware registry.
func RegisteredUnaryServerInterceptors() []grpc.UnaryServerInterceptor {
	middlewareMu.Lock()
	defer middlewareMu.Unlock()
	return unaryMiddlewares
}
