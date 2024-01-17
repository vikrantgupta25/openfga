package graph

import (
	"context"
	"fmt"
	"github.com/karlseguin/ccache/v3"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"strconv"
	"time"
)

var ctx = context.Background()

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient() *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return &RedisClient{
		client: rdb,
	}
}

type RedisCacheResolver struct {
	c            *RedisClient
	delegate     CheckResolver
	cache        *ccache.Cache[*CachedResolveCheckResponse]
	maxCacheSize int64
	cacheTTL     time.Duration
	logger       logger.Logger
	// allocatedCache is used to denote whether the cache is allocated by this struct.
	// If so, CachedCheckResolver is responsible for cleaning up.
	allocatedCache bool
}

// RedisResolverOpt defines an option that can be used to change the behavior of redisCacheCheckResolver
// instance.
type RedisResolverOpt func(*RedisCacheResolver)

// WithLogger sets the logger for the cached check resolver
func WithClient(client *RedisClient) RedisResolverOpt {
	return func(ccr *RedisCacheResolver) {
		ccr.c = client
	}
}

func NewRedisCheckResolver(delegate CheckResolver, opts ...RedisResolverOpt) *RedisCacheResolver {

	checker := &RedisCacheResolver{
		delegate:     delegate,
		maxCacheSize: defaultMaxCacheSize,
		cacheTTL:     defaultCacheTTL,
		logger:       logger.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(checker)
	}

	return checker
}

func (r RedisCacheResolver) furtherFetch(ctx context.Context, req *ResolveCheckRequest, cacheKey string) (*ResolveCheckResponse, error) {
	resp, err := r.delegate.ResolveCheck(ctx, req)
	if err != nil {
		return nil, err
	}

	err = r.c.client.Set(ctx, cacheKey, resp.Allowed, 0).Err()
	if err != nil {
		return nil, err
	}
	return resp, nil

}

func (r RedisCacheResolver) ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	cacheKey, err := CheckRequestCacheKey(req)
	if err != nil {
		r.logger.Error("cache key computation failed with error", zap.Error(err))
		return nil, err
	}

	val, err := r.c.client.Get(ctx, cacheKey).Result()

	switch {
	case err == redis.Nil:
		return r.furtherFetch(ctx, req, cacheKey)
	case err != nil:
		return nil, err
	case val == "":
		fmt.Println("value is empty")
	}

	allowed, err := strconv.ParseBool(val)
	if err != nil {
		return nil, fmt.Errorf("Not understand val", allowed)
	}
	return &ResolveCheckResponse{
		Allowed: allowed,
		ResolutionMetadata: &ResolutionMetadata{
			Depth:               defaultResolveNodeLimit,
			DatastoreQueryCount: 0,
		},
	}, nil

}

func (r RedisCacheResolver) Close() {
	r.c.client.Close()
}
