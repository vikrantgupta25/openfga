package storagewrappers

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

var _ storage.RelationshipTupleReader = (*datastoreThrottlingTupleReader)(nil)

type datastoreThrottlingTupleReader struct {
	storage.RelationshipTupleReader
	throttlingServer *DatastoreThrottlingTupleReaderServer
	readCounter      *atomic.Uint32
	threshold        uint32
}

func NewDatastoreThrottlingTupleReader(wrapped storage.RelationshipTupleReader,
	threshold uint32,
	throttlingServer *DatastoreThrottlingTupleReaderServer) *datastoreThrottlingTupleReader {
	ds := &datastoreThrottlingTupleReader{
		RelationshipTupleReader: wrapped,
		readCounter:             new(atomic.Uint32),
		threshold:               threshold,
		throttlingServer:        throttlingServer,
	}
	return ds
}

// ReadUserTuple tries to return one tuple that matches the provided key exactly.
func (r *datastoreThrottlingTupleReader) ReadUserTuple(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
) (*openfgav1.Tuple, error) {
	err := r.throttleQuery(ctx)
	if err != nil {
		return nil, err
	}
	return r.RelationshipTupleReader.ReadUserTuple(ctx, store, tupleKey)
}

// Read the set of tuples associated with `store` and `TupleKey`, which may be nil or partially filled.
func (r *datastoreThrottlingTupleReader) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey) (storage.TupleIterator, error) {
	err := r.throttleQuery(ctx)
	if err != nil {
		return nil, err
	}

	return r.RelationshipTupleReader.Read(ctx, store, tupleKey)
}

// ReadUsersetTuples returns all userset tuples for a specified object and relation.
func (r *datastoreThrottlingTupleReader) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
) (storage.TupleIterator, error) {
	err := r.throttleQuery(ctx)
	if err != nil {
		return nil, err
	}

	return r.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter)
}

func (r *datastoreThrottlingTupleReader) throttleQuery(ctx context.Context) error {
	currentCount := r.readCounter.Add(1)
	if currentCount > r.threshold && r.throttlingServer != nil {
		start := time.Now()
		delta := (currentCount - r.threshold) / 5
		numWait := min(2^delta, 3000)
		for i := 0; i < int(numWait); i++ {
			end := time.Now()
			timeWaiting := end.Sub(start)
			if timeWaiting < 3*time.Second {
				r.throttlingServer.throttleQuery(ctx)
			} else {
				return fmt.Errorf("waiting too long")
			}
		}
		r.throttlingServer.throttleQuery(ctx)
	}
	return nil
}
