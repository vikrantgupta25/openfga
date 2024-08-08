package authz

import (
	"context"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// ServerInterface is an interface for the server.
type ServerInterface interface {
	CheckWithoutAuthz(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error)
}
