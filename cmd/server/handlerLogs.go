package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs(log routing.GameLog) pubsub.AckType {
	defer fmt.Println("> ", log)
	gamelogic.WriteLog(log)
	return pubsub.AckTypeAck
}
