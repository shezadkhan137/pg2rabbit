package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

func setupRabbitConnection(URI string, exchangeName string) (*amqp.Connection, error) {
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
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)

	if err != nil {
		return nil, err
	}

	return conn, nil
}

func launchRabbitWorkers(parsedMessageChan chan ParsedMessage, conn *amqp.Connection, workerCount int, allCloseChan chan bool) {
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
	allCloseChan <- true
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

func dedupeStream(inputChan, outputChan chan ParsedMessage, timePeriod time.Duration) {
	ticker := time.NewTicker(timePeriod)
	checkMessages := make(map[string]bool)
	messagesThisPeriod := 0

	for {
		select {

		case parsedMessage := <-inputChan:
			// created parsed message key
			key := string(makeKey(parsedMessage))
			if _, ok := checkMessages[key]; !ok {
				// not seen data in this time period
				checkMessages[key] = true
				outputChan <- parsedMessage
			} else {
				log.Printf("found dupe %+v\n", parsedMessage)
			}
			messagesThisPeriod += 1

		case <-ticker.C:
			checkMessages = make(map[string]bool)
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
