package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error creating connection channel: %v", err)
	}

	err = pubsub.PublishJSON(ch,
		routing.ExchangePerilDirect,
		routing.PauseKey,
		routing.PlayingState{
			IsPaused: true,
		},
	)

	if err != nil {
		log.Fatalf("Error publishing: %v", err)
	}

	// Waith for ctrl+c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println()
	fmt.Println("Closing connection and shutting down Peril server")
}
