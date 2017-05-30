package pg2rabbit

import (
	"expvar"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx"
	"github.com/paulbellamy/ratecounter"
)

var (
	incomingMessageRateCounter *ratecounter.RateCounter = ratecounter.NewRateCounter(1 * time.Second)
	incomingMessagesPerSecond                           = expvar.NewInt("incoming_messages_per_second")

	averageParseTimeCounter    *ratecounter.AvgRateCounter = ratecounter.NewAvgRateCounter(1 * time.Second)
	averageParseTimeLastSecond                             = expvar.NewInt("average_parse_time_last_second")
)

type RawMessage struct {
	DataString string
	Received   time.Time
}

func LaunchRDSStream(repConnection *pgx.ReplicationConn, messageChan chan<- RawMessage,
	slotName string, createSlot bool, closeChan chan bool) {

	defer func() {
		close(messageChan)
	}()

	if createSlot {
		err := repConnection.CreateReplicationSlot(slotName, "test_decoding")
		if err != nil {
			log.Fatal(err.Error())
		}
		defer func() {
			log.Printf("dropping replica slot %s\n", slotName)
			repConnection.DropReplicationSlot(slotName)
		}()
	}

	err := repConnection.StartReplication(slotName, 0, -1)
	if err != nil {
		log.Fatal(err.Error())
	}

	var lastWalStart uint64 = 0
	var messageCount int = 0

	for {

		select {
		case <-closeChan:
			log.Println("launchRDSStream: stopping postgres replication")
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
				// Could have some backoff & reconnect here
				panic(err)
			}
		}

		dataString := string(message.WalMessage.WalData)
		messageChan <- RawMessage{DataString: dataString, Received: time.Now()}

		// Analytics
		incomingMessageRateCounter.Incr(1)
		incomingMessagesPerSecond.Set(incomingMessageRateCounter.Rate())
	}
}

func SetupPostgresConnection(URI string) (*pgx.ReplicationConn, error) {
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

func SetupWorkers(messageChan chan RawMessage, parsedMessageChan chan ParsedMessage, workerNumber int) {
	var wg sync.WaitGroup
	wg.Add(workerNumber)
	for w := 1; w <= workerNumber; w++ {
		go processMessageWorker(messageChan, parsedMessageChan, &wg)
	}
	wg.Wait()
	close(parsedMessageChan)
}

func processMessageWorker(messageChan chan RawMessage, parsedMessageChan chan ParsedMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	for m := range messageChan {

		startTime := time.Now()

		parsedMessage, err := DoParse(m)
		if err != nil {
			log.Printf(err.Error())
			continue
		}

		averageParseTimeCounter.Incr(time.Since(startTime).Nanoseconds())
		averageParseTimeLastSecond.Set(int64(averageParseTimeCounter.Rate()))

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
