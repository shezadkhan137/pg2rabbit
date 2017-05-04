package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "expvar"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	PostgresURL     string
	RabbitURL       string
	WorkerCount     int    `default:"1000"`
	RabbitPushCount int    `default:"10"`
	ReplicaSlotName string `default:"db_changes"`
	CreateSlot      bool   `default:"false"`
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

	var messageChan chan RawMessage = make(chan RawMessage)
	var parsedMessageChan chan ParsedMessage = make(chan ParsedMessage)
	var closeChan chan bool = make(chan bool, 1)
	var allClosedChan chan bool = make(chan bool, 1)
	var dedupeChan chan ParsedMessage
	if c.DedupeInterval != 0 {
		dedupeChan = make(chan ParsedMessage)
	} else {
		dedupeChan = parsedMessageChan
	}

	go launchRDSStream(repConnection, messageChan, c.ReplicaSlotName, c.CreateSlot, closeChan)
	go setupWorkers(messageChan, parsedMessageChan, c.WorkerCount)
	go dedupeStream(parsedMessageChan, dedupeChan, time.Duration(c.DedupeInterval)*time.Second)
	go launchRabbitWorkers(c.ExchangeName, dedupeChan, rabbitConn, c.RabbitPushCount, allClosedChan)

	go http.ListenAndServe(":8080", http.DefaultServeMux)

	exitChan := make(chan os.Signal, 2)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan
	log.Println("main: shutting down... please wait")
	closeChan <- true
	<-allClosedChan
	log.Println("main: exiting")
}
