package storagewrappers

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

var _ storage.RelationshipTupleReader = (*delayedTupleReader)(nil)

type delayedTupleReader struct {
	storage.RelationshipTupleReader
	backoff     backoff.BackOff
	readCounter atomic.Uint64
	threshold   uint64
	mu          sync.Mutex
}

// NewDelayedTupleReader constructs a RelationshipTupleReader that is throttled/delayed in
// an exponential fashion after the provided threshold.
//
// The exponential delay is configured to start at 10ms and increase with an exponential
// multiplier of 1.005 up to 3s in total duration.
func NewDelayedTupleReader(
	reader storage.RelationshipTupleReader,
	threshold uint64,
) *delayedTupleReader {

	// todo: we may consider making this configurable
	expoBackoff := backoff.NewExponentialBackOff()
	expoBackoff.MaxElapsedTime = 0
	expoBackoff.InitialInterval = 10 * time.Millisecond
	expoBackoff.Multiplier = 1.005
	expoBackoff.MaxInterval = 3 * time.Second
	expoBackoff.RandomizationFactor = 0
	expoBackoff.Reset() // required to re-establish the InitialInterval - without this we start at 500ms

	d := &delayedTupleReader{
		RelationshipTupleReader: reader,
		readCounter:             atomic.Uint64{},
		threshold:               threshold,
		backoff:                 expoBackoff,
	}

	return d
}

// Read implements storage.RelationshipTupleReader.
func (d *delayedTupleReader) Read(
	ctx context.Context,
	storeID string,
	tupleKey *openfgav1.TupleKey,
) (storage.TupleIterator, error) {
	d.delay()
	return d.RelationshipTupleReader.Read(ctx, storeID, tupleKey)
}

// ReadPage implements storage.RelationshipTupleReader.
func (d *delayedTupleReader) ReadPage(
	ctx context.Context,
	storeID string,
	tupleKey *openfgav1.TupleKey,
	opts storage.PaginationOptions,
) ([]*openfgav1.Tuple, []byte, error) {
	d.delay()
	return d.RelationshipTupleReader.ReadPage(ctx, storeID, tupleKey, opts)
}

// ReadStartingWithUser implements storage.RelationshipTupleReader.
func (d *delayedTupleReader) ReadStartingWithUser(
	ctx context.Context,
	storeID string,
	filter storage.ReadStartingWithUserFilter,
) (storage.Iterator[*openfgav1.Tuple], error) {
	d.delay()
	return d.RelationshipTupleReader.ReadStartingWithUser(ctx, storeID, filter)
}

// ReadUserTuple implements storage.RelationshipTupleReader.
func (d *delayedTupleReader) ReadUserTuple(
	ctx context.Context,
	storeID string,
	tupleKey *openfgav1.TupleKey,
) (*openfgav1.Tuple, error) {
	d.delay()
	return d.RelationshipTupleReader.ReadUserTuple(ctx, storeID, tupleKey)
}

// ReadUsersetTuples implements storage.RelationshipTupleReader.
func (d *delayedTupleReader) ReadUsersetTuples(
	ctx context.Context,
	storeID string,
	filter storage.ReadUsersetTuplesFilter,
) (storage.TupleIterator, error) {
	d.delay()
	return d.RelationshipTupleReader.ReadUsersetTuples(ctx, storeID, filter)
}

// delay pauses for an interval of time defined by the ticker if the read
// counter has exceeded the configured threshold.
func (d *delayedTupleReader) delay() {
	readCount := d.readCounter.Add(1)
	if readCount > d.threshold {
		d.mu.Lock()
		duration := d.backoff.NextBackOff()
		d.mu.Unlock()

		time.Sleep(duration)
	}
}
