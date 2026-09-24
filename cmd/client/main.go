package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	connectionString := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}
	defer connection.Close()

	fmt.Println("Connected to Peril server")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error creating user: %v", err)
	}

	publishRoutingKey := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName)
	publishCh, err := connection.Channel()
	if err != nil {
		log.Fatalf("error creating channel: %v", err)
	}

	gameState := gamelogic.NewGameState(userName)
	pauseQueueName := fmt.Sprintf("%s.%s", routing.PauseKey, userName)
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		pauseQueueName,
		routing.PauseKey,
		pubsub.QueueTransient,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatalf("error subscribing to pause: %v", err)
	}

	moveRoutingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	moveQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, userName)
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		moveQueueName,
		moveRoutingKey,
		pubsub.QueueTransient,
		handlerMove(gameState, publishCh),
	)
	if err != nil {
		log.Fatalf("error subscribing to army move: %v", err)
	}

	warQueueName := "war"
	warRoutingKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		warQueueName,
		warRoutingKey,
		pubsub.QueueDurable,
		handlerWar(gameState, publishCh),
	)

Loop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		switch words[0] {
		case "spawn":
			err := gameState.CommandSpawn(words)
			if err != nil {
				log.Printf("Error spawning unit: %v", err)
			}
			continue
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				log.Printf("Error moving unit: %v", err)
			}

			fmt.Println("Sending move message...")
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				publishRoutingKey,
				move,
			)
			if err != nil {
				log.Printf("Error publishing: %v", err)
			}
			fmt.Println("Army moved")
			continue
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			break Loop
		default:
			fmt.Println("Unknown command")
		}
	}

	// Waith for ctrl+c
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
	// fmt.Println()
	// fmt.Println("Closing Peril client")

}
