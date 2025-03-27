package rabbitmq

import (
	"fmt"
	"github.com/streadway/amqp"
)

type ExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func DeclareExchange(ch *amqp.Channel, cfg ExchangeConfig) error {
	err := ch.ExchangeDeclare(
		cfg.Name,
		cfg.Type,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Internal,
		cfg.NoWait,
		cfg.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %v", err)
	}
	return nil
}

func DeclareAndBindQueue(ch *amqp.Channel, queueCfg QueueConfig, exchangeName, routingKey string) error {
	_, err := ch.QueueDeclare(
		queueCfg.Name,
		queueCfg.Durable,
		queueCfg.AutoDelete,
		queueCfg.Exclusive,
		queueCfg.NoWait,
		queueCfg.Args,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}

	err = ch.QueueBind(
		queueCfg.Name,
		routingKey,
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}

	return nil
}
