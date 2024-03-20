package storagewrappers

import (
	"context"
	"github.com/openfga/openfga/pkg/server/errors"
	"sync/atomic"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const dsThrottlingSpanAttribute = "ds_throttling"

var _ storage.RelationshipTupleReader = (*datastoreThrottlingTupleReader)(nil)

var (
	dsThrottlingReadDelayMsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "datastore_throttling_read_delay_ms",
		Help:                            "Time spent waiting for Read, ReadUserTuple and ReadUsersetTuples calls to the datastore due to throttling",
		Buckets:                         []float64{1, 3, 5, 10, 25, 50, 100, 1000, 5000}, // Milliseconds. Upper bound is config.UpstreamTimeout.
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"grpc_service", "grpc_method"})
)

type datastoreThrottlingTupleReader struct {
	storage.RelationshipTupleReader
	readCounter     *atomic.Uint32
	threshold       uint32
	ticker          *time.Ticker
	throttlingQueue chan struct{}
	done            chan struct{}
}

func NewDatastoreThrottlingTupleReader(wrapped storage.RelationshipTupleReader, threshold uint32) *datastoreThrottlingTupleReader {
	ds := &datastoreThrottlingTupleReader{
		RelationshipTupleReader: wrapped,
		readCounter:             new(atomic.Uint32),
		threshold:               threshold,
		ticker:                  time.NewTicker(10 * time.Millisecond),
		throttlingQueue:         make(chan struct{}),
		done:                    make(chan struct{}),
	}
	//go ds.runTicker()
	return ds
}

func (r *datastoreThrottlingTupleReader) Close() {
	//r.done <- struct{}{}
}

func (r *datastoreThrottlingTupleReader) nonBlockingSend(signalChan chan struct{}) {
	select {
	case signalChan <- struct{}{}:
		// message sent
	default:
		// message dropped
	}
}

func (r *datastoreThrottlingTupleReader) runTicker() {
	for {
		select {
		case <-r.done:
			r.ticker.Stop()
			close(r.done)
			close(r.throttlingQueue)
			return
		case <-r.ticker.C:
			r.nonBlockingSend(r.throttlingQueue)
		}
	}
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
	if currentCount > r.threshold {
		return errors.AuthorizationModelResolutionTooComplex
		/*
			start := time.Now()
			<-r.throttlingQueue
			end := time.Now()
			timeWaiting := end.Sub(start).Milliseconds()

			rpcInfo := telemetry.RPCInfoFromContext(ctx)
			dsThrottlingReadDelayMsHistogram.WithLabelValues(
				rpcInfo.Service,
				rpcInfo.Method,
			).Observe(float64(timeWaiting))

			span := trace.SpanFromContext(ctx)
			span.SetAttributes(attribute.Int64(dsThrottlingSpanAttribute, timeWaiting))

		*/
	}
	return nil
}
