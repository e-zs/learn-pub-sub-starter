package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType bool

const (
	QueueTransient SimpleQueueType = false
	QueueDurable   SimpleQueueType = true
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonData,
		},
	)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	durable := bool(queueType)     // true for Durable
	autoDelete := !bool(queueType) // true for Transient
	exclusive := !bool(queueType)  // true for Transient
	noWait := false

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		noWait,
		nil,
	)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	err = ch.QueueBind(queue.Name, key, exchange, noWait, nil)
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	return ch, queue, nil

}
