package graph

import (
	"context"
	"github.com/karlseguin/ccache/v3"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/redis/go-redis/v9"
	"time"
)

var ctx = context.Background()
var rdb *redis.Client

type RedisClient struct {
	client *redis.Client
}

func NewRedisClient() *RedisClient {
	rdb = redis.NewClient(&redis.Options{
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

func (r RedisCacheResolver) ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (r RedisCacheResolver) Close() {
	//TODO implement me
	panic("implement me")
}
