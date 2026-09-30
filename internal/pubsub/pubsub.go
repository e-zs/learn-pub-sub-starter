package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	QueueDurable SimpleQueueType = iota
	QueueTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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
	handler func(T) AckType,
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

	// if err := ch.Qos(10, 0, false); err != nil {
	// 	return fmt.Errorf("error setting Qos limit")
	// }

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

			ackNack := handler(val)

			switch ackNack {
			case Ack:
				err = message.Ack(false)
				// log.Printf("msg ack")
			case NackRequeue:
				err = message.Nack(false, true)
				// log.Printf("msg nack req")
			case NackDiscard:
				err = message.Nack(false, false)
				// log.Printf("msg nack disc")
			default:
				err = message.Nack(false, false)
			}

			// err = message.Ack(false)
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

	args := amqp.Table{
		"x-dead-letter-exchange": routing.ExchangePerilDead,
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == QueueDurable, // true for Durable
		queueType != QueueDurable, // autodelete false for Durable
		queueType != QueueDurable, // exclusive false for Durable
		false,                     // nowait
		args,                      // args
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

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queuType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, q, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queuType,
	)
	if err != nil {
		return err
	}

	// if err := ch.Qos(10, 0, false); err != nil {
	// 	return fmt.Errorf("error setting Qos limit")
	// }

	messages, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		return fmt.Errorf("error consuming messages: %w", err)
	}

	go func() {
		defer ch.Close()
		for message := range messages {
			var val T
			decoder := gob.NewDecoder(bytes.NewReader(message.Body))
			err := decoder.Decode(&val)
			if err != nil {
				log.Printf("error decoding message: %v", err)
				if nackErr := message.Nack(false, false); nackErr != nil {
					log.Printf("error discarding invalid delivery: %s", nackErr)
				}
				continue
			}

			ackNack := handler(val)

			switch ackNack {
			case Ack:
				err = message.Ack(false)
				// log.Printf("msg ack")
			case NackRequeue:
				err = message.Nack(false, true)
				// log.Printf("msg nack req")
			case NackDiscard:
				err = message.Nack(false, false)
				// log.Printf("msg nack disc")
			default:
				err = message.Nack(false, false)
			}

			// err = message.Ack(false)
			if err != nil {
				log.Printf("error acknowledging delivery: %v", err)
			}
		}
	}()

	return nil
}
