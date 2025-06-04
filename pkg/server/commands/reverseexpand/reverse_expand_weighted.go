package reverseexpand

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
)

func (c *ReverseExpandQuery) loopOverWeightedEdges(
	ctx context.Context,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	needsCheck bool,
	req *ReverseExpandRequest,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
	sourceUserObj string,
) error {
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	var errs error

	for _, edge := range edges {
		r := &ReverseExpandRequest{
			Consistency:      req.Consistency,
			Context:          req.Context,
			ContextualTuples: req.ContextualTuples,
			ObjectType:       req.ObjectType,
			Relation:         req.Relation,
			StoreID:          req.StoreID,
			User:             req.User,

			weightedEdge:        edge,
			weightedEdgeTypeRel: edge.GetTo().GetUniqueLabel(),
			edge:                req.edge,
		}
		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandDirectWeighted(ctx, r, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.ComputedEdge:
			toLabel := edge.GetTo().GetUniqueLabel()

			// turn "document#viewer" into "viewer"
			rel := tuple.GetRelation(toLabel)
			r.User = &UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   sourceUserObj,
					Relation: rel,
				},
			}

			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.TTUEdge:
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandTupleToUsersetWeighted(ctx, r, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.RewriteEdge:
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		default:
			panic("unsupported edge type")
		}
	}

	return errors.Join(errs, pool.Wait())
}

func (c *ReverseExpandQuery) reverseExpandDirectWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		// attribute.String("edge", req.edge.String()),
		attribute.String("source.user", req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	// Do we want to separate buildQueryFilters more, and feed it in below?
	err = c.readTuplesAndExecuteWeighted(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
	return err
}

func (c *ReverseExpandQuery) reverseExpandTupleToUsersetWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUsersetWeighted", trace.WithAttributes(
		attribute.String("edge.from", req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("edge.to", req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("source.user", req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	err = c.readTuplesAndExecuteWeighted(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
	return err
}

func (c *ReverseExpandQuery) readTuplesAndExecuteWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "readTuplesAndExecuteWeighted")
	defer span.End()

	userFilter, relationFilter, objectType := c.buildQueryFiltersWeighted(req)

	// TODO: I think we need to determine the object type in the filters function as well, based on edge type
	// TTUS behave differently from others
	fmt.Printf("JUSTIN Weighted\n"+
		"\trelationFilter: %s\n"+
		"\tuserFilter: %s\n"+
		"\tObject type: %s\n"+
		"\tEdgeType: %d\n",
		relationFilter,
		userFilter,
		objectType,
		req.weightedEdge.GetEdgeType(),
	)

	iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: objectType,     // e.g. directs-employee
		Relation:   relationFilter, // other-rel
		UserFilter: userFilter,
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.Consistency,
		},
	})
	if err != nil {
		return err
	}

	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(c.typesystem),
	)
	defer filteredIter.Stop()

	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	var errs error

LoopOnIterator:
	for {
		tk, err := filteredIter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			errs = errors.Join(errs, err)
			break LoopOnIterator
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		if !condEvalResult.ConditionMet {
			if len(condEvalResult.MissingParameters) > 0 {
				errs = errors.Join(errs, condition.NewEvaluationError(
					tk.GetCondition().GetName(),
					fmt.Errorf("tuple '%s' is missing context parameters '%v'",
						tuple.TupleKeyToString(tk),
						condEvalResult.MissingParameters),
				))
			}

			continue
		}

		foundObject := tk.GetObject()
		fmt.Printf("JUSTIN FOUND OBJECT WEIGHTED: %s\n", foundObject)
		var newRelation string

		switch req.weightedEdge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			err = c.trySendCandidate(ctx, intersectionOrExclusionInPreviousEdges, foundObject, resultChan)
			errs = errors.Join(errs, err)
			continue
		case weightedGraph.TTUEdge:
			// TODO: what's the right behavior here?
			//newRelation = req.weightedEdge.GetTo().GetLabel()

			// so now we need to see if this object has the requisite relation to the parent
			// e.g. we found 'directs:ttu_alg_2', so check if IT has a direct_parent relation to any TTUs
			// what about multiple nested TTUs tho, or a TTU that resolves to a userset
			newRelation = tuple.GetRelation(req.weightedEdge.GetTuplesetRelation())

		default:
			panic("unsupported edge type")
		}

		fmt.Printf("Dispatching after TTU\n \tRelation: %s\n\tObject %s\n", newRelation, foundObject)
		// TODO after lunch: you need this to be the direct edge from ttus -> direct parent
		// Stick the WG back on the window and find out how to get it
		pool.Go(func(ctx context.Context) error {
			return c.dispatch(ctx, &ReverseExpandRequest{
				StoreID:    req.StoreID,
				ObjectType: req.ObjectType,
				Relation:   req.Relation,
				User: &UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   foundObject,
						Relation: newRelation,
					},
					Condition: tk.GetCondition(),
				},
				ContextualTuples: req.ContextualTuples,
				Context:          req.Context,
				edge:             req.edge,
				Consistency:      req.Consistency,
				// TODO: what edge do i give it tho
				weightedEdge:        req.weightedEdge,
				weightedEdgeTypeRel: req.weightedEdgeTypeRel,
			}, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
		})
	}

	errs = errors.Join(errs, pool.Wait())
	if errs != nil {
		telemetry.TraceError(span, errs)
		return errs
	}

	return nil
}

func (c *ReverseExpandQuery) buildQueryFiltersWeighted(
	req *ReverseExpandRequest,
) ([]*openfgav1.ObjectRelation, string, string) {
	var userFilter []*openfgav1.ObjectRelation
	var relationFilter, objectType string

	// Should this actually be looking at the node we're heading towards?
	switch req.weightedEdge.GetEdgeType() {
	case weightedGraph.DirectEdge:
		// the .From() for a direct edge will have a type#rel e.g. directs-employee#other_rel
		fromLabel := req.weightedEdge.GetFrom().GetLabel()
		relationFilter = tuple.GetRelation(fromLabel)                        // directs-employee#other_rel -> other_rel
		objectType = getTypeFromLabel(req.weightedEdge.GetFrom().GetLabel()) // e.g. directs-employee

		toNode := req.weightedEdge.GetTo()

		// e.g. 'user:*'
		if toNode.GetNodeType() == weightedGraph.SpecificTypeWildcard {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: toNode.GetLabel(), // e.g. "employee:*"
			})
		}

		// e.g. 'user:bob'
		if val, ok := req.User.(*UserRefObject); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.BuildObject(val.Object.GetType(), val.Object.GetId()),
			})
		}

		// e.g. 'group:eng#member'
		// so is it if the TO node is direct to a userset?
		// which would be a DirectEdge TO node with type weightedGraph.SpecificTypeAndRelation
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
				userFilter = append(userFilter, val.ObjectRelation)
			} else if toNode.GetNodeType() == weightedGraph.SpecificType {
				userFilter = append(userFilter, &openfgav1.ObjectRelation{
					Object: val.ObjectRelation.GetObject(),
				})
			}
		}
	case weightedGraph.TTUEdge:
		//relationFilter = tuple.GetRelation(req.weightedEdge.GetTuplesetRelation())
		relationFilter = tuple.GetRelation(req.weightedEdge.GetTo().GetLabel())
		objectType = getTypeFromLabel(req.weightedEdge.GetTo().GetLabel())
		// a TTU edge can only have a userset as a source node
		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: val.ObjectRelation.GetObject(),
			})
		} else {
			panic("unexpected source for reverse expansion of tuple to userset")
		}
	default:
		panic("unsupported edge type")
	}

	return userFilter, relationFilter, objectType
}

// expects a "type#rel".
func getTypeFromLabel(label string) string {
	userObject, _ := tuple.SplitObjectRelation(label)
	return userObject
}
