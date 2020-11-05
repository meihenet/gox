package redis

import (
	"context"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

var client *RedisClient
var once sync.Once

type RedisClient struct {
	client *goredis.Client
	ctx    context.Context
}

type redisOption func(*RedisClient)

func WithContext(ctx context.Context) redisOption {
	return func(rc *RedisClient) {
		rc.ctx = ctx
	}
}

func New(url string, opts ...redisOption) (*RedisClient, error) {
	var err error = nil
	once.Do(func() {
		client = &RedisClient{}
		client.ctx = context.Background()
		for _, fun := range opts {
			fun(client)
		}
		err = client.connect(url)
	})
	return client, err
}

// @url: redis://<user>:<password>@<host>:<port>/<db_number>"
func (this *RedisClient) connect(url string) error {
	opt, err := goredis.ParseURL(url)
	if err != nil {
		return err
	}
	this.client = goredis.NewClient(opt)
	return nil
}

func (this *RedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	return this.client.Set(this.ctx, key, value, expiration).Err()
}

func (this *RedisClient) Get(key string) (interface{}, error) {
	return this.client.Get(this.ctx, key).Result()
}
