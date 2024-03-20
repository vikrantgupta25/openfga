package storagewrappers

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
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

type DatastoreThrottlingTupleReaderServer struct {
	ticker          *time.Ticker
	throttlingQueue chan struct{}
	done            chan struct{}
}

func NewDatastoreThrottlingTupleReaderServer(frequency time.Duration) *DatastoreThrottlingTupleReaderServer {
	ds := &DatastoreThrottlingTupleReaderServer{
		ticker:          time.NewTicker(frequency),
		throttlingQueue: make(chan struct{}),
		done:            make(chan struct{}),
	}
	go ds.runTicker()
	return ds
}

func (r *DatastoreThrottlingTupleReaderServer) Close() {
	r.done <- struct{}{}
}

func (r *DatastoreThrottlingTupleReaderServer) nonBlockingSend(signalChan chan struct{}) {
	select {
	case signalChan <- struct{}{}:
		// message sent
	default:
		// message dropped
	}
}

func (r *DatastoreThrottlingTupleReaderServer) runTicker() {
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

func (r *DatastoreThrottlingTupleReaderServer) throttleQuery(ctx context.Context) {
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
}
