package storagewrappers

import (
	"context"
	"sync/atomic"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/throttler"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
)

var (
	datastoreThrottlingDelayMsHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:                       build.ProjectName,
		Name:                            "datastore_throttling_delay_ms",
		Help:                            "Time spent waiting for datastore throttling",
		Buckets:                         []float64{1, 3, 5, 10, 25, 50, 100, 1000, 5000}, // Milliseconds. Upper bound is config.UpstreamTimeout.
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"grpc_service", "grpc_method"})
)

// DatastoreThrottlingClient throttles read requests if the number of requests exceeds prescribed threshold.
type DatastoreThrottlingClient struct {
	storage.RelationshipTupleReader
	datastoreThrottler throttler.Throttler
	readCount          *atomic.Uint32
	hadThrottled       bool
	defaultThreshold   uint32
	maxThreshold       uint32
}

var _ storage.RelationshipTupleReader = (*DatastoreThrottlingClient)(nil)

// NewDatastoreThrottlingClient returns a storage wrapper that will
// throttle requests if the number of read is > threshold.
// Threshold may be overridden via context DatastoreThrottlingThreshold and not exceed maxThreshold.
// Otherwise, defaultThreshold will be used.
func NewDatastoreThrottlingClient(wrapped storage.RelationshipTupleReader,
	defaultThreshold uint32,
	maxThreshold uint32,
	datastoreThrottler throttler.Throttler) *DatastoreThrottlingClient {
	return &DatastoreThrottlingClient{
		RelationshipTupleReader: wrapped,
		datastoreThrottler:      datastoreThrottler,
		defaultThreshold:        defaultThreshold,
		maxThreshold:            maxThreshold,
		readCount:               new(atomic.Uint32),
		hadThrottled:            false,
	}
}

// HadThrottled returns whether request had been throttled due
// to datastore > threshold.
func (r *DatastoreThrottlingClient) HadThrottled() bool {
	return r.hadThrottled
}

func (r *DatastoreThrottlingClient) ReadUserTuple(
	ctx context.Context,
	store string,
	tupleKey *openfgav1.TupleKey,
) (*openfgav1.Tuple, error) {
	r.incrementQueryCount(ctx)
	return r.RelationshipTupleReader.ReadUserTuple(ctx, store, tupleKey)
}

func (r *DatastoreThrottlingClient) Read(ctx context.Context, store string, tupleKey *openfgav1.TupleKey) (storage.TupleIterator, error) {
	r.incrementQueryCount(ctx)
	return r.RelationshipTupleReader.Read(ctx, store, tupleKey)
}

func (r *DatastoreThrottlingClient) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter storage.ReadUsersetTuplesFilter,
) (storage.TupleIterator, error) {
	r.incrementQueryCount(ctx)
	return r.RelationshipTupleReader.ReadUsersetTuples(ctx, store, filter)
}

func (r *DatastoreThrottlingClient) incrementQueryCount(ctx context.Context) {
	currentCount := r.readCount.Add(1)
	threshold := r.defaultThreshold

	maxThreshold := r.maxThreshold
	if maxThreshold == 0 {
		maxThreshold = r.defaultThreshold
	}

	thresholdInCtx := telemetry.DatastoreThrottlingThresholdFromContext(ctx)
	if thresholdInCtx > 0 {
		threshold = min(thresholdInCtx, maxThreshold)
	}

	if currentCount > threshold {
		r.hadThrottled = true

		start := time.Now()
		r.datastoreThrottler.Throttle(ctx)
		end := time.Now()
		timeWaiting := end.Sub(start).Milliseconds()

		rpcInfo := telemetry.RPCInfoFromContext(ctx)
		datastoreThrottlingDelayMsHistogram.WithLabelValues(
			rpcInfo.Service,
			rpcInfo.Method,
		).Observe(float64(timeWaiting))
	}
}
