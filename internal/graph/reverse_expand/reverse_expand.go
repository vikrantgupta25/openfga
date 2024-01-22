package reverseexpand

import (
	"context"
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/sourcegraph/conc/pool"

	"github.com/openfga/openfga/internal/graph"
	dispatchv1alpha1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

const (
	defaultResolutionDepthLimit = 25
)

type localReverseExpander struct {
	tupleReader            storage.RelationshipTupleReader
	delegate               graph.ReverseExpander
	resolutionDepthLimit   uint64
	resolutionBreadthLimit uint64
}

var _ graph.ReverseExpander = (*localReverseExpander)(nil)

type LocalReverseExpanderOpt func(*localReverseExpander)

func WithResolutionBreadthLimit(breadth uint64) LocalReverseExpanderOpt {
	return func(lre *localReverseExpander) {
		lre.resolutionBreadthLimit = breadth
	}
}

func WithMaxResolutionDepth(maxDepth uint64) LocalReverseExpanderOpt {
	return func(lre *localReverseExpander) {
		lre.resolutionDepthLimit = maxDepth
	}
}

// NewLocalReverseExpander constructs a ReverseExpander that reverse
// expands subproblems locally using the provided RelationshipTupleReader
// and ReverseExpander to delegate subproblems to.
func NewLocalReverseExpander(
	tupleReader storage.RelationshipTupleReader,
	opts ...LocalReverseExpanderOpt,
) *localReverseExpander {
	lre := &localReverseExpander{
		tupleReader:            tupleReader,
		resolutionDepthLimit:   25,
		resolutionBreadthLimit: 30,
	}

	lre.delegate = lre

	for _, opt := range opts {
		opt(lre)
	}

	return lre
}

func (lre *localReverseExpander) ReverseExpand(
	req *dispatchv1alpha1.ReverseExpandRequest,
	stream graph.ReverseExpandStream,
) error {
	ctx := stream.GetContext()

	resolutionMetadata := req.GetResolutionMetadata()
	if resolutionMetadata.GetResolutionDepth() >= lre.resolutionDepthLimit {
		return graph.ErrResolutionDepthExceeded
	}

	typesys := typesystem.New(req.GetModel())

	// should we have to construct graph from typesys? or should it just exist it on (lazily of course)
	//
	// e.g. typesys.RelationshipGraph().GetPrunedRelationshipEdges(...)
	// first invocation to .RelationshipGraph() will build it, and
	// subsequent invocations of GetPrunedRelationshipEdges will build the
	// graph edges (lazily)
	//
	// what's the right seperation of concerns here?

	g := graph.New(typesys)

	targetObjectRef := &openfgav1.RelationReference{
		Type: req.GetTargetObjectType(),
		RelationOrWildcard: &openfgav1.RelationReference_Relation{
			Relation: req.GetTargetRelation(),
		},
	}

	sourceUser := req.GetSourceUser().UserRef

	var sourceUserRef *openfgav1.RelationReference

	objectUserRef, ok := sourceUser.(*dispatchv1alpha1.UserRef_Object)
	if ok {
		sourceUserRef = &openfgav1.RelationReference{
			Type: objectUserRef.Object.GetType(),
		}
	}

	typedWildcardUserRef, ok := sourceUser.(*dispatchv1alpha1.UserRef_TypedWildcard)
	if ok {
		sourceUserRef = &openfgav1.RelationReference{
			Type: typedWildcardUserRef.TypedWildcard.GetType(),
			RelationOrWildcard: &openfgav1.RelationReference_Wildcard{
				Wildcard: &openfgav1.Wildcard{},
			},
		}
	}

	usersetUserRef, ok := sourceUser.(*dispatchv1alpha1.UserRef_Userset)
	if ok {
		sourceUserRef = &openfgav1.RelationReference{
			Type: usersetUserRef.Userset.GetObject().GetType(),
			RelationOrWildcard: &openfgav1.RelationReference_Relation{
				Relation: usersetUserRef.Userset.GetRelation(),
			},
		}
	}

	edges, err := g.GetPrunedRelationshipEdges(
		targetObjectRef,
		sourceUserRef,
	)
	if err != nil {
		return err
	}

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithFirstError()
	pool.WithMaxGoroutines(int(lre.resolutionBreadthLimit))

	var errs error

EdgeLoop:
	for _, edge := range edges {
		edge := edge

		switch edge.Type {
		case graph.DirectEdge:
			pool.Go(func(ctx context.Context) error {
				return lre.reverseExpandDirect(&dispatchv1alpha1.ReverseExpandRequest{
					StoreId:            req.GetStoreId(),
					Model:              req.GetModel(),
					TargetObjectType:   edge.TargetReference.Type,
					TargetRelation:     edge.TargetReference.GetRelation(),
					SourceUser:         req.GetSourceUser(),
					ContextualTuples:   req.GetContextualTuples(),
					Context:            req.GetContext(),
					ResolutionMetadata: req.GetResolutionMetadata(),
				}, stream)
			})
		case graph.ComputedUsersetEdge:

			rewrittenReq := &dispatchv1alpha1.ReverseExpandRequest{
				StoreId:            req.GetStoreId(),
				Model:              req.GetModel(),
				TargetObjectType:   req.GetTargetObjectType(),
				TargetRelation:     edge.TargetReference.GetRelation(),
				SourceUser:         req.GetSourceUser(),
				ContextualTuples:   req.GetContextualTuples(),
				Context:            req.GetContext(),
				ResolutionMetadata: resolutionMetadata,
			}

			if err := lre.dispatch(rewrittenReq, stream); err != nil {
				errs = errors.Join(errs, err)
				break EdgeLoop
			}
		case graph.TupleToUsersetEdge:
			pool.Go(func(ctx context.Context) error {
				return fmt.Errorf("not implemented")
			})
		default:
			panic("unsupported edge type")
		}
	}

	err = pool.Wait()
	if err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}

func (lre *localReverseExpander) reverseExpandDirect(
	req *dispatchv1alpha1.ReverseExpandRequest,
	stream graph.ReverseExpandStream,
) error {

	ctx := stream.GetContext()

	combinedTupleReader := storagewrappers.NewCombinedTupleReader(lre.tupleReader, req.GetContextualTuples())

	iter, err := combinedTupleReader.ReadStartingWithUser(ctx, req.GetStoreId(), storage.ReadStartingWithUserFilter{
		ObjectType: req.GetTargetObjectType(),
		Relation:   req.GetTargetRelation(),
		//UserFilter: userFilter,
	})

	//atomic.AddUint32(resolutionMetadata.QueryCount, 1)
	// if err != nil {
	// 	return err
	// }

	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		func(tupleKey *openfgav1.TupleKey) bool {
			return validation.ValidateCondition(req., tupleKey) == nil
		},
	)
	defer filteredIter.Stop()

	pool := pool.New().WithContext(ctx)
	pool.WithCancelOnError()
	pool.WithFirstError()
	pool.WithMaxGoroutines(int(lre.resolutionBreadthLimit))

	var errs error

IterLoop:
	for {
		tk, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			errs = errors.Join(errs, err)
			break IterLoop
		}

		newSourceUserObjectType, newSourceUserObjectID := tuple.SplitObject(tk.GetObject())
		newSourceUserRelation := tk.GetRelation()

		newSourceUserObject := &openfgav1.Object{
			Type: newSourceUserObjectType,
			Id:   newSourceUserObjectID,
		}

		var newSourceUser *dispatchv1alpha1.UserRef
		if newSourceUserRelation != "" {
			newSourceUser = &dispatchv1alpha1.UserRef{
				UserRef: &dispatchv1alpha1.UserRef_Userset{
					Userset: &dispatchv1alpha1.Userset{
						Object:   newSourceUserObject,
						Relation: newSourceUserRelation,
					},
				},
			}
		} else {
			newSourceUser = &dispatchv1alpha1.UserRef{
				UserRef: &dispatchv1alpha1.UserRef_Object{
					Object: newSourceUserObject,
				},
			}
		}

		// avoid the the subsequent dispatch if you've found the
		// terminal subjects you're looking for

		pool.Go(func(ctx context.Context) error {
			return lre.dispatch(&dispatchv1alpha1.ReverseExpandRequest{
				StoreId:            req.GetStoreId(),
				TargetObjectType:   req.GetTargetObjectType(),
				TargetRelation:     req.GetTargetRelation(),
				SourceUser:         newSourceUser,
				ContextualTuples:   req.GetContextualTuples(),
				Context:            req.GetContext(),
				ResolutionMetadata: &dispatchv1alpha1.ReverseExpandResolutionMetadata{},
			}, stream)
		})
	}

	err = pool.Wait()
	if err != nil {
		errs = errors.Join(errs, err)
	}

	return errs
}

// dispatch simply delegates to the internal ReverseExpander delegate
// this localReverseExpander is wired up with.
func (lre *localReverseExpander) dispatch(
	req *dispatchv1alpha1.ReverseExpandRequest,
	stream graph.ReverseExpandStream,
) error {

	resolutionMetadata := req.GetResolutionMetadata()

	// todo: there should be a helper for this
	r := &dispatchv1alpha1.ReverseExpandRequest{
		StoreId:          req.GetStoreId(),
		Model:            req.GetModel(),
		TargetObjectType: req.GetTargetObjectType(),
		TargetRelation:   req.GetTargetRelation(),
		SourceUser:       req.GetSourceUser(),
		ContextualTuples: req.GetContextualTuples(),
		Context:          req.GetContext(),
		ResolutionMetadata: &dispatchv1alpha1.ReverseExpandResolutionMetadata{
			DispatchCount:   resolutionMetadata.GetDispatchCount() + 1,
			QueryCount:      resolutionMetadata.GetQueryCount(),
			ResolutionDepth: resolutionMetadata.GetResolutionDepth(),
		},
	}

	return lre.delegate.ReverseExpand(r, stream)
}
