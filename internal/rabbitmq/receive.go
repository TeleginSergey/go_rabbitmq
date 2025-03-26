package rabbitmq

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

type Consumer struct {
	conn    *amqp.Connection
	Channel *amqp.Channel
	Done    chan error
	tag     string
	queue   string // Добавляем поле для хранения имени очереди
}

func NewConsumer(amqpURI string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		Channel: nil,
		Done:    make(chan error),
		tag:     generateConsumerTag(),
	}

	var err error
	c.conn, err = ConnectWithRetry(amqpURI, 10, 3*time.Second)
	if err != nil {
		return nil, err
	}

	c.Channel, err = c.conn.Channel()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func generateConsumerTag() string {
	rand.Seed(time.Now().UnixNano())
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func (c *Consumer) StartConsuming(
	exchangeName string,
	queueName string,
	consumerName string,
	handler func([]byte) error,
) error {
	// Сначала объявляем exchange
	err := c.Channel.ExchangeDeclare(
		exchangeName,
		"fanout",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	// Затем объявляем очередь
	_, err = c.Channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}

	// Привязываем очередь к exchange
	err = c.Channel.QueueBind(
		queueName,
		"", // routing key
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}

	err = c.Channel.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %v", err)
	}

	deliveries, err := c.Channel.Consume(
		queueName,
		c.tag,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %v", err)
	}

	go func() {
		for d := range deliveries {
			err := handler(d.Body)
			if err != nil {
				log.Printf("[%s] Error handling message: %s - message will be requeued", consumerName, err)
				d.Nack(false, true)
				continue
			}
			if err := d.Ack(false); err != nil {
				log.Printf("[%s] Failed to acknowledge message: %v", consumerName, err)
			}
		}
		log.Printf("[%s] Deliveries channel closed", consumerName)
		c.Done <- nil
	}()

	log.Printf("[%s] Consumer started with tag: %s", consumerName, c.tag)
	return nil
}

func (c *Consumer) Shutdown() error {
	if c.Channel != nil {
		// Сначала отменяем consumer
		if c.tag != "" {
			if err := c.Channel.Cancel(c.tag, false); err != nil {
				log.Printf("Error canceling consumer %s: %v", c.tag, err)
			}
		}
		// Затем закрываем канал
		if err := c.Channel.Close(); err != nil {
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
