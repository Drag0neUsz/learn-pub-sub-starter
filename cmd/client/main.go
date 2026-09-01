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
	fmt.Println("Starting Peril client...")

	cnctStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(cnctStr)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Failed to get client name: %v", err)
	}
	fmt.Println("Welcome to Peril,", username)

	gameState := gamelogic.NewGameState(username)

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	err = pubsub.Subscribe(conn, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), fmt.Sprintf("%s.*", routing.PauseKey), pubsub.SimpleQueueTypeTransient, handlerPause(gameState), pubsub.UnmarshalJSON[routing.PlayingState])
	if err != nil {
		log.Fatalf("Failed to subscribe to pause queue: %v", err)
	}
	err = pubsub.Subscribe(conn, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), pubsub.SimpleQueueTypeTransient, handlerMove(gameState, publishCh), pubsub.UnmarshalJSON[gamelogic.ArmyMove])
	if err != nil {
		log.Fatalf("Failed to subscribe to army moves queue: %v", err)
	}
	err = pubsub.Subscribe(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), pubsub.SimpleQueueTypeDurable, handlerConsumeWar(gameState, publishCh), pubsub.UnmarshalJSON[gamelogic.RecognitionOfWar])
	if err != nil {
		log.Fatalf("Failed to subscribe to war recognitions queue: %v", err)
	}

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		if input[0] == "spawn" {
			err := gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			continue
		}
		if input[0] == "move" {
			moveResult, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println("Error:", err)
				continue
			}
			pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), moveResult)
			if err != nil {
				fmt.Println("Failed to publish move result:", err)
				continue
			}
			fmt.Println("Successfully moved units & published move result:", moveResult.Units, "to", moveResult.ToLocation)
			continue
		}
		if input[0] == "status" {
			gameState.CommandStatus()
			continue
		}
		if input[0] == "help" {
			gamelogic.PrintClientHelp()
			continue
		}
		if input[0] == "spam" {
			err := handlerSpam(publishCh, input, username)
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				fmt.Println("Successfully spammed", input[1], "game logs")
			}
			continue
		}
		if input[0] == "quit" {
			gamelogic.PrintQuit()
			break
		}
		fmt.Println("Invalid input")
		continue
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
