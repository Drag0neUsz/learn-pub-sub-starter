package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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

func PublishGOB[T any](ch *amqp.Channel, exchange, key string, val T) error {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(val)
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
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
	return err
}

func Subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	ch.Qos(10, 0, false)
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	go func() {
		defer ch.Close()
		for msg := range msgs {
			val, err := unmarshaller(msg.Body)
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

func UnmarshalJSON[T any](data []byte) (T, error) {
	var val T
	err := json.Unmarshal(data, &val)
	return val, err
}

func UnmarshalGOB[T any](data []byte) (T, error) {
	var val T
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(&val)
	return val, err
}
