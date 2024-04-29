package telemetry

import (
	"context"
)

type rpcContextName string

type datastoreThrottleThresholdType uint32

const (
	rpcInfoContextName rpcContextName = "rpcInfo"
	Throttled          string         = "Throttled"
)

const (
	datastoreThrottlingThreshold datastoreThrottleThresholdType = iota
)

type RPCInfo struct {
	Method  string
	Service string
}

// ContextWithRPCInfo will save the rpc method and service information in context.
func ContextWithRPCInfo(ctx context.Context, rpcInfo RPCInfo) context.Context {
	return context.WithValue(ctx, rpcInfoContextName, rpcInfo)
}

// RPCInfoFromContext returns method and service stored in context.
func RPCInfoFromContext(ctx context.Context) RPCInfo {
	rpcInfo, ok := ctx.Value(rpcInfoContextName).(RPCInfo)
	if ok {
		return rpcInfo
	}
	return RPCInfo{
		Method:  "unknown",
		Service: "unknown",
	}
}

// ContextWithDatastoreThrottlingThreshold will save the datastore throttling threshold in context.
func ContextWithDatastoreThrottlingThreshold(ctx context.Context, threshold uint32) context.Context {
	return context.WithValue(ctx, datastoreThrottlingThreshold, threshold)
}

// DatastoreThrottlingThresholdFromContext returns the datastore throttling threshold saved in context
// Return 0 if not found.
func DatastoreThrottlingThresholdFromContext(ctx context.Context) uint32 {
	thresholdInContext := ctx.Value(datastoreThrottlingThreshold)
	if thresholdInContext != nil {
		thresholdInInt, ok := thresholdInContext.(uint32)
		if ok {
			return thresholdInInt
		}
	}
	return 0
}
