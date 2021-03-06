package redix

import "context"

func NewCluster(url string, opts ...redisOption) (*RedisClient, error) {
	client := &RedisClient{}
	client.ctx = context.Background()
	for _, fun := range opts {
		fun(client)
	}
	err := client.connect(url)
	return client, err
}
