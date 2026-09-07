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
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"

	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}
	defer connection.Close()

	fmt.Println("Connected to Peril server")

	gamelogic.PrintServerHelp()

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error creating connection channel: %v", err)
	}

	routingKey := fmt.Sprintf("%s.*", routing.GameLogSlug)

	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routingKey,
		pubsub.QueueDurable,
	)
	if err != nil {
		log.Fatalf("Error declaring and binding queue: %v", err)
	}

Loop:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Printf("Error publishing: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Printf("Error publishing: %v", err)
			}
		case "quit":
			fmt.Println("Exiting...")
			break Loop
		default:
			fmt.Println("Unknown command")
		}
	}

	// Wait for ctrl+c
	// signalChan := make(chan os.Signal, 1)
	// signal.Notify(signalChan, os.Interrupt)
	// <-signalChan
	// fmt.Println()
	// fmt.Println("Closing connection and shutting down Peril server")
}
