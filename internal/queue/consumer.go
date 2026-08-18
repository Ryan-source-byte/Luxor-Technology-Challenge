package queue

import (
	"context"
	"encoding/json"

	"luxor-challenge/internal/stats"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

// NewConsumer prepares the processor side of RabbitMQ. I declare the same queue
// here too, so startup order is not fragile🤔.
func NewConsumer(url, queueName string) (*Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	// A small prefetch gives Postgres some breathing room if writes slow down.
	if err := ch.Qos(10, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}

	return &Consumer{conn: conn, ch: ch, queue: queueName}, nil
}

// Run is the async stats loop. The important bit for me is ack-after-write:
// if the DB write fails, the message should not disappear.
func (c *Consumer) Run(ctx context.Context, recorder stats.Recorder) error {
	deliveries, err := c.ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-deliveries:
			if !ok {
				return nil
			}

			var event stats.Event
			if err := json.Unmarshal(msg.Body, &event); err != nil {
				// Bad JSON is not likely to become valid after a retry.
				_ = msg.Nack(false, false)
				continue
			}

			if err := recorder.Record(ctx, event); err != nil {
				// DB errors may be temporary, so let RabbitMQ redeliver later.
				_ = msg.Nack(false, true)
				continue
			}
			_ = msg.Ack(false)
		}
	}
}

func (c *Consumer) Close() error {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
