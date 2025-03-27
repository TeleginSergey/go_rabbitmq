package rabbitmq

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/streadway/amqp"
)

type Consumer struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	Done        chan error
	db          *sql.DB
	redisClient *redis.Client
}

func NewConsumer(amqpURI string, db *sql.DB) (*Consumer, error) {
	conn, err := ConnectWithRetry(amqpURI, 10, 3*time.Second)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	redisClient, err := connectToRedis()
	if err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	return &Consumer{
		conn:        conn,
		channel:     channel,
		Done:        make(chan error),
		db:          db,
		redisClient: redisClient,
	}, nil
}

func generateMessageID(body []byte) string {
	h := sha256.New()
	h.Write(body)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (c *Consumer) StartConsuming(
	exchangeName string,
	queueName string,
	consumerName string,
) error {
	queueCfg := QueueConfig{
		Name:       queueName,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
	if err := DeclareAndBindQueue(c.channel, queueCfg, exchangeName, ""); err != nil {
		return err
	}

	if err := c.channel.Qos(10, 0, false); err != nil {
		return fmt.Errorf("qos setup failed: %v", err)
	}

	deliveries, err := c.channel.Consume(
		queueName,
		consumerName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume failed: %v", err)
	}

	go c.handleDeliveries(deliveries, consumerName)
	log.Printf("[%s] Consumer started", consumerName)
	return nil
}

func (c *Consumer) handleDeliveries(deliveries <-chan amqp.Delivery, consumerName string) {
	for d := range deliveries {
		ctx := context.Background()
		message := string(d.Body)
		messageID := generateMessageID(d.Body)
		redisKey := fmt.Sprintf("message:%s", messageID)

		status, err := c.redisClient.HGet(ctx, redisKey, "status").Result()

		if err != nil && !errors.Is(err, redis.Nil) {
			log.Printf("[%s] Redis error: %v (Nack)", consumerName, err)
			_ = d.Nack(false, true)
			continue
		}

		if status == "processed" {
			if err := c.SaveToDBOnly(consumerName, messageID, message); err != nil {
				log.Printf("[%s] DB insert error: %v (Nack)", consumerName, err)
				_ = d.Nack(false, true)
				continue
			}
			log.Printf("[%s] Message already processed, saved to DB: %s", consumerName, message)
			_ = d.Ack(false)
			continue
		}

		if status == "processing" {
			log.Printf("[%s] Message is being processed by another consumer: %s",
				consumerName, message)
			_ = d.Nack(false, true)
			continue
		}

		if ok, err := c.redisClient.HSetNX(ctx, redisKey, "status", "processing").Result(); err != nil || !ok {
			log.Printf("[%s] Failed to set processing status: %v (Nack)", consumerName, err)
			_ = d.Nack(false, true)
			continue
		}

		c.redisClient.Expire(ctx, redisKey, 24*time.Hour)
		c.redisClient.HSet(ctx, redisKey, "consumer", consumerName)

		if err := c.SaveToDBAndRedis(consumerName, messageID, message); err != nil {
			c.redisClient.HDel(ctx, redisKey, "status")
			log.Printf("[%s] Processing error: %v (Nack)", consumerName, err)
			_ = d.Nack(false, true)
			continue
		}

		log.Printf("[%s] Successfully processed message: %s", consumerName, message)
		_ = d.Ack(false)
	}
}

func (c *Consumer) Shutdown() error {
	if c.redisClient != nil {
		if err := c.redisClient.Close(); err != nil {
			log.Printf("Error closing Redis client: %v", err)
		}
	}
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			log.Printf("Error closing channel: %v", err)
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}
	return nil
}
