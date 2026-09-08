package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	ch, q, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	messages, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		return fmt.Errorf("error consuming messages: %w", err)
	}

	go func() {
		defer ch.Close()
		for message := range messages {
			var val T
			err := json.Unmarshal(message.Body, &val)
			if err != nil {
				log.Printf("error unmarshaling: %v", err)
				continue
			}

			handler(val)

			err = message.Ack(false)
			if err != nil {
				log.Printf("error acknowledging delivery: %v", err)
			}
		}
	}()

	return nil
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
		return nil, amqp.Queue{}, fmt.Errorf("error creating channel: %v", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == QueueDurable, // true for Durable
		queueType != QueueDurable, // autodelete false for Durable
		queueType != QueueDurable, // exclusive false for Durable
		false,                     // nowait
		nil,                       // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error declaring queue: %v", err)
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("error binding queue: %v", err)
	}

	return ch, queue, nil

}
