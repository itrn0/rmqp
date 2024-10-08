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
	ErrContextDone = errors.New("context done")
)

type Message struct {
	ExchangeName string
	RoutingKey   string
	Data         []byte
	ContentType  string
}

type Publisher struct {
	ctx      context.Context
	options  *Options
	messages chan Message
	Url      string
}

func NewPublisher(
	ctx context.Context,
	url string,
	options *Options,
) *Publisher {
	options = handleOptions(options)
	return &Publisher{
		ctx:      ctx,
		Url:      url,
		messages: make(chan Message, options.Capacity),
		options:  options,
	}
}

func (p *Publisher) Publish(msg Message) error {
	if msg.ContentType == "" {
		msg.ContentType = p.options.ContentType
	}
	select {
	case <-p.ctx.Done():
		return ErrContextDone
	case p.messages <- msg:
		return nil
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
		slog.Debug(fmt.Sprintf("reconnecting to RabbitMQ after %v second", p.options.ReconnectDelay.Seconds()))
		time.Sleep(p.options.ReconnectDelay)
	}
}

func (p *Publisher) connect() error {
	cfg := amqp.Config{
		Heartbeat:  p.options.Heartbeat,
		Properties: amqp.Table{},
	}
	if p.options.ConnectionName != "" {
		cfg.Properties["connection_name"] = p.options.ConnectionName
	}
	conn, err := amqp.DialConfig(p.Url, cfg)
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
		case msg, msgOk := <-p.messages:
			if !msgOk {
				return fmt.Errorf("channel closed")
			}
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
