package main

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

func setupRabbitConnection(URI string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(URI)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	err = ch.ExchangeDeclare(
		"database_changes", // name
		"topic",            // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)

	if err != nil {
		return nil, err
	}

	return conn, nil
}

func launchRabbitWorkers(parsedMessageChan chan ParsedMessage, conn *amqp.Connection, workerCount int) {
	var wg sync.WaitGroup
	for w := 1; w <= workerCount; w++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Println(err.Error())
			continue
		}
		go launchRabbitPush(parsedMessageChan, ch)
		wg.Add(workerCount)
	}
	wg.Wait()
}

func launchRabbitPush(parsedMessageChan chan ParsedMessage, ch *amqp.Channel) {
	for parsedMessage := range parsedMessageChan {

		jsonData, err := json.Marshal(parsedMessage)
		if err != nil {
			log.Println("Could not marshal data")
			continue
		}
		err = ch.Publish(
			"database_changes",  // exchange
			parsedMessage.Table, // routing key
			false,               // mandatory
			false,               // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(jsonData),
			})

		log.Printf("TABLE: %s, OP: %s, ID: %+v\n", parsedMessage.Table, parsedMessage.Op, parsedMessage.Data["id"])
		if err != nil {
			log.Println("Failed to publish to rabbit")
		}
	}
}
