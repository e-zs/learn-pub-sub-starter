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

	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, userName)

	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.QueueTransient,
	)
	if err != nil {
		log.Fatalf("Error declaring and binding queue: %v", err)
	}

	gameState := gamelogic.NewGameState(userName)

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
			_, err := gameState.CommandMove(words)
			if err != nil {
				log.Printf("Error moving unit: %v", err)
			}
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
