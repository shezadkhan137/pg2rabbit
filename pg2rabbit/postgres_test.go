package pg2rabbit

import (
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx"
	. "github.com/smartystreets/goconvey/convey"
)

func slotIsAlive(conn *pgx.Conn) bool {
	rows, _ := conn.Query("SELECT active FROM pg_replication_slots WHERE slot_name = 'test_slot' LIMIT 1")
	var active bool
	for rows.Next() {
		err := rows.Scan(&active)
		if err != nil {
			panic(err)
		}
		return true
	}
	return false
}

func drainChan(ch chan RawMessage) {
	for {
		select {
		case <-ch:
			return
		default:
			return
		}
	}
}

func TestPostgresReading(t *testing.T) {

	Convey("given a valid postgres database with replication slot", t, func() {

		dbURL := "postgres:///test_db"
		config, err := pgx.ParseURI(dbURL)
		if err != nil {
			panic(err)
		}

		conn, err := pgx.Connect(config)
		if err != nil {
			panic(err)
		}

		slotName := "test_slot"

		conn.Exec("SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding')")
		conn.Exec("CREATE TABLE tasks (id serial primary key, description TEXT NOT NULL)")

		Convey("check that SetupPostgresConnection returns valid connection", func() {

			replicaConnection, err := SetupPostgresConnection(dbURL)

			Convey("check that err is not returned", func() {
				So(err, ShouldBeNil)

				Convey("check connection is not nil", func() {
					So(replicaConnection, ShouldNotBeNil)

					Convey("when that connection is given to LaunchRDSStream", func() {

						messageChan := make(chan RawMessage)
						closeChan := make(chan bool, 1)

						go LaunchRDSStream(replicaConnection, messageChan, slotName, false, closeChan)

						Convey("check LaunchRDSStream keeps the connection alive", func() {

							Convey("when a insert is made", func() {
								_, _ = conn.Exec("insert into tasks(description) values($1)", "hello world")

								Convey("and we wait two seconds", func() {

									time.Sleep(2 * time.Second)

									Convey("check than connection is still alive", func() {
										So(slotIsAlive(conn), ShouldBeTrue)
										So(replicaConnection.IsAlive(), ShouldBeTrue)

										Convey("after message chan is drained", func() {

											drainChan(messageChan)

											Convey("check connection is still alive", func() {

												So(slotIsAlive(conn), ShouldBeTrue)
											})

										})

									})
								})
							})

						})

						Convey("check on INSERT the correct raw message is received", nil)

						Convey("check on UPDATE that the correct raw message is received", nil)

						Convey("check on DELETE the correct raw message is received", nil)

						Convey("check that when LaunchRDSStream exits, the slot is destroyed", func() {

						})

						Reset(func() {
							fmt.Println("resetting")
							closeChan <- true
							fmt.Println("closing")
							for {
								fmt.Println("looping")
								_, more := <-messageChan
								if !more {
									return
								}
							}
						})

					})

				})
			})

			Reset(func() {
				fmt.Println("closing replica connection")
				replicaConnection.Close()
			})
		})

		Reset(func() {
			fmt.Println("dropping slot")
			conn.Exec("select pg_drop_replication_slot('test_slot')")
			conn.Exec("DROP TABLE tasks")
		})

	})
}
