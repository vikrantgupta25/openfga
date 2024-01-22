package reverseexpand

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/graph"
	dispatchv1alpha1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
	"github.com/openfga/openfga/pkg/storage"
)

func TestReverseExpand(t *testing.T) {

	var stream graph.ReverseExpandStream

	var objects sync.Map

	dedupStream := graph.NewStreamWithContext(
		context.Background(),
		stream,
		func(result *dispatchv1alpha1.ReverseExpandResponse) (*dispatchv1alpha1.ReverseExpandResponse, bool, error) {
			object := result.GetObject()
			if _, exists := objects.LoadOrStore(object, object); exists {
				return result, false, nil
			}

			return result, true, nil
		},
	)

	var tupleReader storage.RelationshipTupleReader

	var lre graph.ReverseExpander = NewLocalReverseExpander(tupleReader)

	err := lre.ReverseExpand(
		&dispatchv1alpha1.ReverseExpandRequest{},
		dedupStream,
	)
	require.NoError(t, err)
}
