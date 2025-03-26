package rabbitmq

import (
	"bufio"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewPublisher(amqpURI string) (*Publisher, error) {
	conn, err := ConnectWithRetry(amqpURI, 10, 3*time.Second)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Объявляем exchange один раз при создании publisher
	err = channel.ExchangeDeclare(
		"messages",
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Publisher{conn: conn, channel: channel}, nil
}

func (p *Publisher) PublishFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
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
				ContentType: "text/plain",
				Body:        []byte(message),
			})
		if err != nil {
			return err
		}
		log.Printf("Published: %s", message)
	}
	confirmChan := make(chan amqp.Confirmation, 1)
	p.channel.NotifyPublish(confirmChan)

	for scanner.Scan() {
		message := scanner.Text()
		err = p.channel.Publish(
			"messages",
			"",
			true,  // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message),
			})

		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}

		// Ждем подтверждения от RabbitMQ
		if confirmed := <-confirmChan; !confirmed.Ack {
			return fmt.Errorf("failed to confirm message publication")
		}

		log.Printf("Published and confirmed: %s", message)
	}

	return scanner.Err()
}

func (p *Publisher) Shutdown() error {
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			return err
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil {
			return err
		}
	}
	return nil
}
