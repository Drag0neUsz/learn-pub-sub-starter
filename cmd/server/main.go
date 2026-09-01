package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	cnctStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(cnctStr)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %v", err)
	}
	defer publishCh.Close()

	err = pubsub.SubscribeGOB(conn, routing.ExchangePerilTopic, fmt.Sprintf("%s", routing.GameLogSlug), fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.SimpleQueueTypeDurable, handlerLogs)
	if err != nil {
		log.Fatalf("Failed to declare and bind queue: %v", err)
	}

	// wait for ctrl+c
	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		if input[0] == "pause" {
			fmt.Println("Sending pause message...")
			err := pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				fmt.Println("Failed to publish pause message:", err)
				continue
			}
			continue
		}
		if input[0] == "resume" {
			fmt.Println("Sending resume message...")
			err := pubsub.PublishJSON(publishCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				fmt.Println("Failed to publish resume message:", err)
				continue
			}
			continue
		}
		if input[0] == "quit" {
			fmt.Println("Quitting...")
			break
		}
		fmt.Println("Invalid input")
		continue
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Shutting down...")
}
