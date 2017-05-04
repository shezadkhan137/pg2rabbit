package pg2rabbit

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestParse(t *testing.T) {

	// Only pass t into top-level Convey calls
	Convey("Given a valid update message with table, operation and column data", t, func() {

		validMessage := `table public.users_usersociallogin: UPDATE: id[integer]:32 type[character varying]:'facebook' social_id[character varying]:'10154308642985348' access_token[bytea]:'hello' user_id[integer]:129`
		rawMessage := RawMessage{DataString: validMessage, Received: time.Now()}

		Convey("When message is parsed", func() {
			parsedOutput := parse(rawMessage.DataString)

			Convey("The array of strings is space seperated, taking into account quotes and brackets", func() {
				expectedOutput := []string{
					"table",
					" public.users_usersociallogin:",
					" UPDATE:",
					" id[integer]:32",
					" type[character varying]:'facebook'",
					" social_id[character varying]:'10154308642985348'",
					" access_token[bytea]:'hello'",
					" user_id[integer]:129",
				}

				So(parsedOutput, ShouldResemble, expectedOutput)

				Convey("When toStruct is called with parsedOutput", func() {

					parsedMessage, err := toStruct(parsedOutput, rawMessage.Received)

					Convey("Expect that there is not a error", func() {
						So(err, ShouldBeNil)

						Convey("Expect that parsedMessage has the correct table", func() {
							So(parsedMessage.Table, ShouldEqual, "public.users_usersociallogin")
						})

						Convey("Expect that parsedMessage has the correct operation", func() {
							So(parsedMessage.Op, ShouldEqual, "UPDATE")
						})

						Convey("Expect that Data contains the correct information", func() {
							expectedData := map[string]DataCol{
								"id": {
									Name:  "id",
									Type:  "integer",
									Value: "32",
								},
								"access_token": {
									Name:  "access_token",
									Type:  "bytea",
									Value: "'hello'",
								},
								"social_id": {
									Name:  "social_id",
									Type:  "character varying",
									Value: "'10154308642985348'",
								},
								"type": {
									Name:  "type",
									Type:  "character varying",
									Value: "'facebook'",
								},
								"user_id": {
									Name:  "user_id",
									Type:  "integer",
									Value: "129",
								},
							}
							So(parsedMessage.Data, ShouldResemble, expectedData)
						})
					})
				})
			})
		})
	})

	Convey("Given a invalid message which does not contain any spaces, quotes or brackets", t, func() {

		invalidMessage := `HELLO-WORLD`
		rawMessage := RawMessage{DataString: invalidMessage, Received: time.Now()}

		Convey("When message is parsed", func() {
			parsedOutput := parse(rawMessage.DataString)

			Convey("Check that parsedOutput is equal to input", func() {
				So(parsedOutput, ShouldResemble, []string{invalidMessage})
			})

			Convey("When toStruct is called with parsedOutput", func() {
				_, err := toStruct(parsedOutput, rawMessage.Received)

				Convey("Check that an error is not nil", func() {
					So(err, ShouldNotBeNil)
				})
			})
		})
	})
}
