package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerSpam(ch *amqp.Channel, words []string, username string) error {
	if len(words) != 2 {
		fmt.Println("Usage: spam <n>")
		return errors.New("usage: spam <n>")
	}
	n, err := strconv.Atoi(words[1])
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		log := routing.GameLog{
			CurrentTime: time.Now(),
			Message:     gamelogic.GetMaliciousLog(),
			Username:    username,
		}
		err := pubsub.PublishGameLog(ch, log)
		if err != nil {
			return err
		}
	}
	return nil
}
