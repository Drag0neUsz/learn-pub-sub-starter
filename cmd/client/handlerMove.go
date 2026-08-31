package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.AckTypeAck
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.AckTypeNackDiscard
		case gamelogic.MoveOutcomeMakeWar:
			key := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username)
			data := gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, key, data)
			if err != nil {
				fmt.Printf("Error publishing war recognition: %v\n", err)
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck
		default:
			return pubsub.AckTypeNackDiscard
		}
	}
}
