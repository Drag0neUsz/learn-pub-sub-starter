package pubsub

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	SimpleQueueTypeDurable   SimpleQueueType = "durable"
	SimpleQueueTypeTransient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {

	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	dur := false
	if queueType == SimpleQueueTypeDurable {
		dur = true
	}
	q, err := ch.QueueDeclare(
		queueName,
		dur,
		!dur,
		!dur,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		fmt.Printf("3: %v\n", err)
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(
		queueName,
		key,
		exchange,
		false,
		nil,
	)
	return ch, q, err
}
