package queue

import (
	"context"
	"encoding/json"

	"luxor-challenge/internal/stats"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

// NewPublisher is the server-side stats outlet for queue mode. I wanted the
// server to just say "record this submit" and not know the RabbitMQ details.
func NewPublisher(url, queueName string) (*Publisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	// Durable queue plus persistent messages gives the bonus path a basic
	// restart story without making the code too heavy.
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	return &Publisher{conn: conn, ch: ch, queue: queueName}, nil
}

// Record implements stats.Recorder, so direct DB mode and RabbitMQ mode stay
// behind the same small interface.
func (p *Publisher) Record(ctx context.Context, event stats.Event) error {
	if event.Count <= 0 {
		event.Count = 1
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.ch.PublishWithContext(ctx, "", p.queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (p *Publisher) Close(context.Context) error {
	if p.ch != nil {
		_ = p.ch.Close()
	}
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}
