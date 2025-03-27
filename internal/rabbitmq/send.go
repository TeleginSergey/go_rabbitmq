package rabbitmq

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type Publisher struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
}

func NewPublisher(amqpURI string) (*Publisher, error) {
	conn, err := ConnectWithRetry(amqpURI, 10, 3*time.Second)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	if err := channel.Confirm(false); err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("channel could not be put into confirm mode: %v", err)
	}

	exchangeCfg := ExchangeConfig{
		Name:       "messages",
		Type:       "fanout",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
	if err := DeclareExchange(channel, exchangeCfg); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	return &Publisher{
		conn:     conn,
		channel:  channel,
		confirms: channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}, nil
}

func (p *Publisher) PublishFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		message := scanner.Text()

		err = p.channel.Publish(
			"messages",
			"",
			false,
			false,
			amqp.Publishing{
				ContentType:  "text/plain",
				Body:         []byte(message),
				DeliveryMode: amqp.Persistent,
			})

		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}

		confirmed := <-p.confirms
		if !confirmed.Ack {
			return fmt.Errorf("failed to confirm message publication")
		}

		log.Printf("Published and confirmed: %s", message)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %v", err)
	}

	return nil
}

func (p *Publisher) Shutdown() error {
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			log.Printf("Error closing channel: %v", err)
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			log.Printf("Error closing connection: %v", err)
		}
	}
	return nil
}
