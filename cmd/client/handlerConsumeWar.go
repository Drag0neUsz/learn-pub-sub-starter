package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
)

func handlerConsumeWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(body gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, _, _ := gs.HandleWar(body)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.AckTypeNackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.AckTypeNackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return pubsub.AckTypeAck
		case gamelogic.WarOutcomeYouWon:
			return pubsub.AckTypeAck
		case gamelogic.WarOutcomeDraw:
			return pubsub.AckTypeAck
		default:
			fmt.Println("Error: unknown war outcome")
			return pubsub.AckTypeNackDiscard
		}
	}
}
