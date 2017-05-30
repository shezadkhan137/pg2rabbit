package pg2rabbit

import (
	"log"
	"time"
)

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
