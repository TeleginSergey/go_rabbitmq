package rabbitmq

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/streadway/amqp"
	"log"
	"time"
)

func ConnectWithRetry(amqpURI string, maxRetries int, delay time.Duration) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	for i := 0; i < maxRetries; i++ {
		conn, err = amqp.Dial(amqpURI)
		if err == nil {
			return conn, nil
		}

		log.Printf("Attempt %d/%d: failed to connect to RabbitMQ: %v", i+1, maxRetries, err)
		time.Sleep(delay)
	}

	return nil, fmt.Errorf("failed to connect after %d attempts: %v", maxRetries, err)
}

func connectToRedis() (*redis.Client, error) {
	var client *redis.Client
	var err error

	for i := 0; i < 5; i++ {
		client = redis.NewClient(&redis.Options{
			Addr:     "redis:6379",
			Password: "",
			DB:       0,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = client.Ping(ctx).Result()
		if err == nil {
			return client, nil
		}

		time.Sleep(time.Duration(i+1) * time.Second)
	}

	return nil, fmt.Errorf("failed to connect to Redis after 5 attempts: %v", err)
}

func (c *Consumer) SaveToDBOnly(consumerName, messageID, content string) error {
	_, err := c.db.Exec(
		fmt.Sprintf(`INSERT INTO messages_%s 
            (message_id, content, processed_at)
            VALUES ($1, $2, NOW()) 
            ON CONFLICT (message_id) DO NOTHING`, consumerName),
		messageID, content,
	)
	return err
}

func (c *Consumer) SaveToDBAndRedis(consumerName, messageID, content string) error {
	if err := c.SaveToDBOnly(consumerName, messageID, content); err != nil {
		return err
	}

	ctx := context.Background()
	redisKey := fmt.Sprintf("message:%s", messageID)
	if err := c.redisClient.HSet(ctx, redisKey,
		"status", "processed",
		"result", content,
		"consumer", consumerName,
	).Err(); err != nil {
		return err
	}

	return c.redisClient.Expire(ctx, redisKey, 24*time.Hour).Err()
}
