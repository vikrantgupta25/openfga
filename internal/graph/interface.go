//go:generate mockgen -source interface.go -destination ./mock_check_resolver.go -package graph CheckResolver

package graph

import (
	"context"

	dispatchv1alpha1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
)

// CheckResolver represents an interface that can be implemented to provide recursive resolution
// of a Check.
type CheckResolver interface {
	ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error)
	Close()
}

type ReverseExpander interface {
	ReverseExpand(
		req *dispatchv1alpha1.ReverseExpandRequest,
		stream ReverseExpandStream,
	) error
}

// ReverseExpandStream is a stream which returns ReverseExpandResponse
// values.
type ReverseExpandStream Stream[*dispatchv1alpha1.ReverseExpandResponse]

// Stream represents a generic mechanism to stream/send results and
// provide a way to fetch the underlying context for the stream.
type Stream[T any] interface {

	// Send sends the generic value over the stream and returns
	// an error if the value could not be sent.
	Send(T) error

	// GetContext returns the stream's context.
	GetContext() context.Context
}

// StreamProcessorFunc defines a function that can be implemented
// to provide stream processing.
//
// The boolean return value should be used to indicate if the result
// provided should be published on the stream. A boolean false will
// indicate the result should not be published, while a boolean true
// will lead to result being published.
type StreamProcessorFunc[T any] func(result T) (T, bool, error)

type WrappedStreamWithContext[T any] struct {
	Stream        Stream[T]
	Context       context.Context
	ProcessorFunc StreamProcessorFunc[T]
}

var _ Stream[any] = (*WrappedStreamWithContext[any])(nil)

// NewStreamWithContext returns a wrapped stream that wraps
// the ctx and stream s. The wrapped stream applies the
// provided [StreamProcessorFunc] to the stream before
// sending a value on the stream. If the processor function
// returns a boolean true, then we
func NewStreamWithContext[T any](
	ctx context.Context,
	s Stream[T],
	fn StreamProcessorFunc[T],
) Stream[T] {
	return &WrappedStreamWithContext[T]{
		Stream:        s,
		Context:       ctx,
		ProcessorFunc: fn,
	}
}

// GetContext implements Stream.
func (*WrappedStreamWithContext[T]) GetContext() context.Context {
	panic("unimplemented")
}

// Send implements Stream.
func (*WrappedStreamWithContext[T]) Send(T) error {
	panic("unimplemented")
}
