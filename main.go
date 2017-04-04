package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	PostgresURL     string
	RabbitURL       string
	WorkerCount     int    `default:"1000"`
	RabbitPushCount int    `default:"10"`
	ReplicaSlotName string `default:"db_changes"`
	ExchangeName    string `default:"database_changes"`
	DedupeInterval  int    `default:"1"`
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

	rabbitConn, err := setupRabbitConnection(c.RabbitURL, c.ExchangeName)
	if err != nil {
		log.Fatal(err.Error())
	}

	var messageChan chan string = make(chan string)
	var parsedMessageChan chan ParsedMessage = make(chan ParsedMessage)
	var closeChan chan chan bool = make(chan chan bool)
	var dedupeChan chan ParsedMessage
	if c.DedupeInterval != 0 {
		dedupeChan = make(chan ParsedMessage)
	} else {
		dedupeChan = parsedMessageChan
	}

	go launchRDSStream(repConnection, messageChan, c.ReplicaSlotName, closeChan)
	go setupWorkers(messageChan, parsedMessageChan, c.WorkerCount)
	go dedupeStream(parsedMessageChan, dedupeChan, time.Duration(c.DedupeInterval)*time.Second)
	go launchRabbitWorkers(dedupeChan, rabbitConn, c.RabbitPushCount)

	exitChan := make(chan os.Signal, 2)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan
	log.Println("Shutting down...")
	cChan := make(chan bool)
	closeChan <- cChan
	<-cChan
	log.Println("Exiting main")
}
