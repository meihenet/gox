package redix

import (
	"context"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

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

var client *RedisClient
var once sync.Once

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

// Set the value for key
// @key - string
// @value - interface{}
// @seconds - the seconds of expiration (optional, by default it wont be expired forever)
func (this *RedisClient) Set(key string, value interface{}, seconds ...int) error {
	expiration := time.Duration(0) * time.Second
	if len(seconds) > 0 {
		expiration = time.Duration(seconds[0]) * time.Second
	}
	return this.client.Set(this.ctx, key, value, expiration).Err()
}

// Get the value by key
func (this *RedisClient) Get(key string) (interface{}, error) {
	return this.client.Get(this.ctx, key).Result()
}
