package graph

import (
	"context"
	"log"

	"github.com/openfga/openfga/pkg/tuple"
)

type cycleCheckResolver struct {
	delegate CheckResolver
}

// Close implements CheckResolver.
func (*cycleCheckResolver) Close() {
	panic("unimplemented")
}

func NewCycleCheckResolver() *cycleCheckResolver {
	c := &cycleCheckResolver{}
	c.delegate = c

	return c
}

// ResolveCheck implements CheckResolver.
func (c *cycleCheckResolver) ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {

	key := tuple.TupleKeyToString(req.GetTupleKey())

	if req.VisitedPaths == nil {
		req.VisitedPaths = map[string]struct{}{}
	}

	log.Printf("visited_paths %v\n", req.VisitedPaths)

	if _, ok := req.VisitedPaths[key]; ok {
		log.Printf("CYCLE DETECTED '%s'\n", key)
		return nil, ErrCycleDetected
	}

	req.VisitedPaths[key] = struct{}{}

	return c.delegate.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              req.GetStoreID(),
		AuthorizationModelID: req.GetAuthorizationModelID(),
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.GetContextualTuples(),
		ResolutionMetadata:   req.GetResolutionMetadata(), // operate on copy
		//VisitedPaths:         maps.Clone(req.VisitedPaths),
		VisitedPaths: req.VisitedPaths,
		Context:      req.GetContext(),
	})
}

func (c *cycleCheckResolver) SetDelegate(delegate CheckResolver) {
	c.delegate = delegate
}

var _ CheckResolver = (*cycleCheckResolver)(nil)
