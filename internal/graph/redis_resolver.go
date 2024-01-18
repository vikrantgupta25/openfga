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
	ttl    time.Duration
}

func NewRedisClient(addr string, password string, ttl time.Duration) *RedisClient {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password, // no password set
		DB:       0,        // use default DB
	})

	return &RedisClient{
		client: rdb,
		ttl:    ttl,
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
	r.logger.Info("redis cache miss")

	resp, err := r.delegate.ResolveCheck(ctx, req)
	if err != nil {
		return nil, err
	}

	err = r.c.client.Set(ctx, cacheKey, resp.Allowed, r.c.ttl).Err()
	if err != nil {
		r.logger.Error("redis cache set failed", zap.Error(err))

		return nil, err
	}

	r.logger.Info("redis cache set")

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
		r.logger.Error("redis fetch fail", zap.Error(err))
		return nil, err
	case val == "":
		r.logger.Error("redis vale empty")

		fmt.Println("value is empty")
	}

	r.logger.Info("redis able to retrieve from cache")

	allowed, err := strconv.ParseBool(val)
	if err != nil {
		r.logger.Error("redis failed to parse value", zap.String("val", val))

		return nil, fmt.Errorf("not understand val", allowed)
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
