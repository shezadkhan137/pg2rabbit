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
	"github.com/shezadkhan137/pg2rabbit/pg2rabbit"
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

	repConnection, err := pg2rabbit.SetupPostgresConnection(c.PostgresURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	rabbitConn, err := pg2rabbit.SetupRabbitConnection(c.RabbitURL, c.ExchangeName)
	if err != nil {
		log.Fatal(err.Error())
	}

	var messageChan chan pg2rabbit.RawMessage = make(chan pg2rabbit.RawMessage)
	var parsedMessageChan chan pg2rabbit.ParsedMessage = make(chan pg2rabbit.ParsedMessage)
	var closeChan chan bool = make(chan bool, 1)
	var allClosedChan chan bool = make(chan bool, 1)
	var dedupeChan chan pg2rabbit.ParsedMessage
	if c.DedupeInterval != 0 {
		dedupeChan = make(chan pg2rabbit.ParsedMessage)
	} else {
		dedupeChan = parsedMessageChan
	}

	go pg2rabbit.LaunchRDSStream(repConnection, messageChan, c.ReplicaSlotName, c.CreateSlot, closeChan)
	go pg2rabbit.SetupWorkers(messageChan, parsedMessageChan, c.WorkerCount)
	go pg2rabbit.DedupeStream(parsedMessageChan, dedupeChan, time.Duration(c.DedupeInterval)*time.Second)
	go pg2rabbit.LaunchRabbitWorkers(c.ExchangeName, dedupeChan, rabbitConn, c.RabbitPushCount, allClosedChan)

	go http.ListenAndServe(":8080", http.DefaultServeMux)

	exitChan := make(chan os.Signal, 2)
	signal.Notify(exitChan, os.Interrupt, syscall.SIGTERM)
	<-exitChan
	log.Println("main: shutting down... please wait")
	closeChan <- true
	<-allClosedChan
	log.Println("main: exiting")
}
