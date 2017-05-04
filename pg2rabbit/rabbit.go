package pg2rabbit

import (
	"encoding/json"
	"expvar"
	"log"
	"sync"
	"time"

	"github.com/paulbellamy/ratecounter"
	"github.com/streadway/amqp"
)

var (
	publishedMessageRateCounter *ratecounter.RateCounter = ratecounter.NewRateCounter(1 * time.Second)
	publishedMessagesPerSecond                           = expvar.NewInt("published_messages_per_second")

	averageTimeToPublishCounter    *ratecounter.AvgRateCounter = ratecounter.NewAvgRateCounter(1 * time.Second)
	averageTimeToPublishLastSecond                             = expvar.NewInt("average_time_to_publish_last_second")
)

func SetupRabbitConnection(URI string, exchangeName string) (*amqp.Connection, error) {
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

func LaunchRabbitWorkers(exchangeName string, parsedMessageChan chan ParsedMessage, conn *amqp.Connection, workerCount int, allCloseChan chan bool) {
	var wg sync.WaitGroup
	for w := 1; w <= workerCount; w++ {
		ch, err := conn.Channel()
		if err != nil {
			log.Println(err.Error())
			continue
		}
		wg.Add(1)
		go rabbitPushWorker(exchangeName, parsedMessageChan, ch, &wg, w)
	}
	wg.Wait()
	log.Printf("launchRabbitWorkers: all workeers stopped\n")
	allCloseChan <- true
}

func rabbitPushWorker(exchangeName string, parsedMessageChan chan ParsedMessage, ch *amqp.Channel, wg *sync.WaitGroup, workerNumber int) {

	defer func() {
		wg.Done()
	}()

	for parsedMessage := range parsedMessageChan {

		jsonData, err := json.Marshal(parsedMessage)
		if err != nil {
			log.Println("Could not marshal data")
			continue
		}
		err = ch.Publish(
			exchangeName,        // exchange
			parsedMessage.Table, // routing key
			false,               // mandatory
			false,               // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(jsonData),
			})

		log.Printf("TABLE: %s, OP: %s, ID: %+v\n", parsedMessage.Table, parsedMessage.Op, parsedMessage.Data["id"])
		if err != nil {
			log.Println("Failed to publish to rabbit")
		}

		// Analytics
		publishedMessageRateCounter.Incr(1)
		publishedMessagesPerSecond.Set(publishedMessageRateCounter.Rate())
		averageTimeToPublishCounter.Incr(time.Since(parsedMessage.Received).Nanoseconds())
		averageTimeToPublishLastSecond.Set(int64(averageTimeToPublishCounter.Rate()))
	}

	log.Printf("launchRabbitPush (worker %d): stopping\n", workerNumber)
}

func DedupeStream(inputChan, outputChan chan ParsedMessage, timePeriod time.Duration) {
	ticker := time.NewTicker(timePeriod)
	checkMessages := make(map[string]bool)
	messagesThisPeriod := 0

	for {
		select {

		case parsedMessage, ok := <-inputChan:
			if !ok {
				log.Printf("dedupeStream: stopping")
				close(outputChan)
				return
			}
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
