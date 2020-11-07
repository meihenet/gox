package redix

import (
	"context"
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

func New(url string, opts ...redisOption) (*RedisClient, error) {
	client := &RedisClient{}
	client.ctx = context.Background()
	for _, fun := range opts {
		fun(client)
	}
	err := client.connect(url)
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

// Search all keys by pattern
func (this *RedisClient) Keys(pattern string) ([]string, error) {
	var keys []string
	values, err := this.client.Keys(this.ctx, pattern).Result()
	if err == nil {
		for _, key := range values {
			keys = append(keys, string(key))
		}
	}
	return keys, err
}

// Set the value for key
// @key - string
// @value - interface{}
// @seconds - the seconds of expiration (optional, by default it wont be expired forever)
func (this *RedisClient) Set(key string, value interface{}, expirations ...time.Duration) error {
	expiration := time.Duration(0) * time.Second
	if len(expirations) > 0 {
		expiration = time.Duration(expirations[0]) * time.Second
	}
	return this.client.Set(this.ctx, key, value, expiration).Err()
}

// Set the value for key with expiration
func (this *RedisClient) SetEX(key string, value interface{}, expiration time.Duration) error {
	return this.client.SetEX(this.ctx, key, value, expiration).Err()
}

// Set the value for key with expiration
// if value exists, it wont overwrite the old value
func (this *RedisClient) SetNX(key string, value interface{}, expiration time.Duration) error {
	return this.client.SetNX(this.ctx, key, value, expiration).Err()
}

// Set values for Multiple Keys
// or accept a Map with string keys
func (this *RedisClient) MSet(values ...interface{}) (bool, error) {
	result, err := this.client.MSet(this.ctx, values...).Result()
	return (result == "OK"), err
}

// Get the value by key
func (this *RedisClient) Get(key string) (string, error) {
	return this.client.Get(this.ctx, key).Result()
}

// Get the values for Multiple Keys
func (this *RedisClient) MGet(keys ...string) ([]interface{}, error) {
	return this.client.MGet(this.ctx, keys...).Result()
}

// GetSet
func (this *RedisClient) GetSet(key string, value interface{}) (string, error) {
	return this.client.GetSet(this.ctx, key, value).Result()
}

// Delete the values by key or keys
func (this *RedisClient) Del(keys ...string) (int64, error) {
	return this.client.Del(this.ctx, keys...).Result()
}

// Delete the values by key or keys
func (this *RedisClient) Unlink(keys ...string) (int64, error) {
	return this.client.Unlink(this.ctx, keys...).Result()
}

// Check the keys if exists
func (this *RedisClient) Exists(keys ...string) (int64, error) {
	return this.client.Exists(this.ctx, keys...).Result()
}

// expire, pexpire
func (this *RedisClient) Expire(key string, expiration time.Duration) (bool, error) {
	return this.client.Expire(this.ctx, key, expiration).Result()
}

func (this *RedisClient) PExpire(key string, expiration time.Duration) (bool, error) {
	return this.client.PExpire(this.ctx, key, expiration).Result()
}

// ttl,pttl
func (this *RedisClient) TTL(key string) (time.Duration, error) {
	return this.client.TTL(this.ctx, key).Result()
}

func (this *RedisClient) PTTL(key string) (time.Duration, error) {
	return this.client.PTTL(this.ctx, key).Result()
}

// expireat, pexpireat
func (this *RedisClient) ExpireAt(key string, tm time.Time) (bool, error) {
	return this.client.ExpireAt(this.ctx, key, tm).Result()
}

func (this *RedisClient) PExpireAt(key string, tm time.Time) (bool, error) {
	return this.client.PExpireAt(this.ctx, key, tm).Result()
}

// persist
func (this *RedisClient) Persist(key string) (bool, error) {
	return this.client.Persist(this.ctx, key).Result()
}

// dump the values of key
func (this *RedisClient) Dump(key string) (string error) {
	return this.client.Dump(this.ctx, key).Result()
}

// Rename
// whether newKey exists or not, this will be work!
func (this *RedisClient) Rename(key string, newKey string) (bool error) {
	return this.client.Rename(this.ctx, key, newKey).Result()
}

// RenameNX
// only works when newKey doesn't exist
func (this *RedisClient) RenameNX(key string, newKey string) (bool error) {
	return this.client.RenameNX(this.ctx, key, newKey).Result()
}

// type
func (this *RedisClient) Type(key string) (string, error) {
	return this.client.Type(this.ctx, key).Result()
}

// Get a random key from current database
func (this *RedisClient) RandomKey() (string, error) {
	return this.client.RandomKey(this.ctx).Result()
}

// Move key to database ID
func (this *RedisClient) Move(key string, db int) (bool, error) {
	return this.client.Move(this.ctx, key, db).Result()
}

/*
// Scan the Databases to search keys
func (this *RedisClient) Scan(cursor uint64, match string, count int64) error {
}
*/
