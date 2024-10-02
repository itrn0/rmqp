package rmqp

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log/slog"
	"time"
)

var (
	ErrTimeout     = errors.New("publish timeout")
	ErrContextDone = errors.New("context done")
)

type Message struct {
	ExchangeName string
	RoutingKey   string
	Data         []byte
	ContentType  string
}

type Publisher struct {
	Url      string
	ctx      context.Context
	timeout  time.Duration
	messages chan Message
}

func NewPublisher(
	ctx context.Context,
	url string,
	capacity int,
	timeout time.Duration,
) *Publisher {
	return &Publisher{
		ctx:      ctx,
		Url:      url,
		messages: make(chan Message, capacity),
		timeout:  timeout,
	}
}

func (p *Publisher) Publish(msg Message) error {
	if msg.ContentType == "" {
		msg.ContentType = "text/plain"
	}
	select {
	case <-p.ctx.Done():
		return ErrContextDone
	case p.messages <- msg:
		return nil
	case <-time.After(p.timeout):
		return ErrTimeout
	}
}

func (p *Publisher) Start() {
	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}
		if err := p.connect(); err != nil {
			slog.Warn("RabbitMQ connect error", "err", err)
		}
		slog.Debug("reconnecting to RabbitMQ after 3 second")
		time.Sleep(3 * time.Second)
	}
}

func (p *Publisher) connect() error {
	conn, err := amqp.Dial(p.Url)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel: %w", err)
	}
	defer ch.Close()

	slog.Info("connected to RabbitMQ")
	defer slog.Info("disconnected from RabbitMQ")

	for {
		select {
		case <-p.ctx.Done():
			return ErrContextDone
		case msg := <-p.messages:
			err = ch.Publish(
				msg.ExchangeName,
				msg.RoutingKey,
				false,
				false,
				amqp.Publishing{
					ContentType: msg.ContentType,
					Body:        msg.Data,
				},
			)
			if err != nil {
				return fmt.Errorf("failed to publish: %w", err)
			}
		}
	}
}
