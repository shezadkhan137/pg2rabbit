package main

import (
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx"
)

func launchRDSStream(repConnection *pgx.ReplicationConn, messageChan chan string, slotName string, closeChan chan chan bool) {

	err := repConnection.StartReplication(slotName, 0, -1)
	if err != nil {
		log.Fatal(err.Error())
	}

	var lastWalStart uint64 = 0
	var messageCount int = 0

	for {

		select {
		case cChan := <-closeChan:
			close(messageChan)
			log.Println("Exiting RDS Loop")
			cChan <- true
			return
		default:
		}

		message, err := repConnection.WaitForReplicationMessage(time.Duration(10) * time.Millisecond)
		if err != nil {
			if err == pgx.ErrNotificationTimeout {
				err2 := sendStandby(repConnection, lastWalStart)
				if err2 != nil {
					panic(err2)
				}
				continue
			}
			// Could have some backoff & reconnect here
			panic(err)
		}

		if message.ServerHeartbeat != nil {
			continue
		}

		lastWalStart = message.WalMessage.WalStart
		messageCount += 1

		if messageCount%100 == 0 {
			err := sendStandby(repConnection, lastWalStart)
			if err != nil {
				panic(err)
			}
		}

		dataString := string(message.WalMessage.WalData)
		messageChan <- dataString
	}
}

func setupPostgresConnection(URI string) (*pgx.ReplicationConn, error) {
	config, err := pgx.ParseURI(URI)
	if err != nil {
		return nil, err
	}
	config.RuntimeParams["replication"] = "logical"

	repConnection, err := pgx.ReplicationConnect(config)
	if err != nil {
		return nil, err
	}

	return repConnection, nil
}

func setupWorkers(messageChan chan string, parsedMessageChan chan ParsedMessage, number int) {
	var wg sync.WaitGroup
	wg.Add(number)
	for w := 1; w <= number; w++ {
		go processMessageWorker(messageChan, parsedMessageChan, &wg)
	}
	wg.Wait()
	close(parsedMessageChan)
}

func processMessageWorker(messageChan chan string, parsedMessageChan chan ParsedMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	for m := range messageChan {
		parsedMessage, err := doParse(m)
		if err != nil {
			log.Printf(err.Error())
			continue
		}
		if parsedMessage != nil {
			parsedMessageChan <- *parsedMessage
		}
	}
}

func sendStandby(repConnection *pgx.ReplicationConn, WalStart uint64) (err error) {
	standbyStatus, err := pgx.NewStandbyStatus(WalStart)
	if err != nil {
		return err
	}

	err = repConnection.SendStandbyStatus(standbyStatus)
	if err != nil {
		return err
	}
	return err
}
