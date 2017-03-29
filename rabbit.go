package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"github.com/willf/bloom"
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

	inputChan := parsedMessageChan
	outputChan := make(chan ParsedMessage)

	go dedupeStream(inputChan, outputChan)

	for w := 1; w <= workerCount; w++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Println(err.Error())
			continue
		}
		go launchRabbitPush(outputChan, ch)
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

func dedupeStream(inputChan, outputChan chan ParsedMessage) {
	timePeriod := time.Second * 1
	ticker := time.NewTicker(timePeriod)
	bloomFilter := bloom.NewWithEstimates(10000, 0.000001)
	messagesThisPeriod := 0

	for {
		select {

		case parsedMessage := <-inputChan:
			// created parsed message key
			key := makeKey(parsedMessage)
			if !bloomFilter.Test(key) {
				// not seen data in this time period
				bloomFilter.Add(key)
				outputChan <- parsedMessage
			} else {
				log.Printf("found dupe %+v\n", parsedMessage)
			}
			messagesThisPeriod += 1

		case <-ticker.C:
			bloomFilter.ClearAll()
			// calculate messages / second
			messageRate := float64(messagesThisPeriod) / timePeriod.Seconds()
			log.Printf("message rate is %.2f/s\n", messageRate)
			messagesThisPeriod = 0
		}
	}
}

func makeKey(parsedMessage ParsedMessage) []byte {
	b := append([]byte(parsedMessage.Table), []byte(parsedMessage.Op)...)
	return append(b, []byte(parsedMessage.Data["id"].Value)...)
}
