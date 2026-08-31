package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonData,
		},
	)
	return err
}

type AckType string

const (
	AckTypeAck         AckType = "Ack"
	AckTypeNackRequeue AckType = "NackRequeue"
	AckTypeNackDiscard AckType = "NackDiscard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		defer ch.Close()
		for msg := range msgs {
			var val T
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				continue
			}
			ackType := handler(val)
			switch ackType {
			case AckTypeAck:
				msg.Ack(false)
			case AckTypeNackRequeue:
				msg.Nack(false, true)
			case AckTypeNackDiscard:
				msg.Nack(false, false)

			}
		}
	}()

	return nil
}
