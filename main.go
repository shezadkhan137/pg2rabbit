package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	PostgresURL     string
	RabbitURL       string
	WorkerCount     int    `default:"1000"`
	RabbitPushCount int    `default:"10"`
	ReplicaSlotName string `default:"db_changes"`
}

func main() {

	var c Config
	err := envconfig.Process("", &c)
	if err != nil {
		log.Fatal(err.Error())
	}

	repConnection, err := setupPostgresConnection(c.PostgresURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	rabbitConn, err := setupRabbitConnection(c.RabbitURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	var messageChan chan string = make(chan string)
	var parsedMessageChan chan ParsedMessage = make(chan ParsedMessage)
	var closeChan chan chan bool = make(chan chan bool)

	go setupWorkers(messageChan, parsedMessageChan, c.WorkerCount)
	go launchRabbitWorkers(parsedMessageChan, rabbitConn, c.RabbitPushCount)
	go launchRDSStream(repConnection, messageChan, c.ReplicaSlotName, closeChan)

	exitChan := make(chan os.Signal, 2)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan
	log.Println("Shutting down...")
	cChan := make(chan bool)
	closeChan <- cChan
	<-cChan
	log.Println("Exiting main")
}
