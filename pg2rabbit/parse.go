package pg2rabbit

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"
)

type DataCol struct {
	Name  string
	Type  string
	Value string
}

type ParsedMessage struct {
	Table    string             `json:"table"`
	Op       string             `json:"operation"`
	Data     map[string]DataCol `json:"data"`
	Received time.Time
}

func DoParse(dataMessage RawMessage) (*ParsedMessage, error) {
	dataString := dataMessage.DataString

	if strings.HasPrefix(dataString, "COMMIT") || strings.HasPrefix(dataString, "BEGIN") {
		return nil, nil
	}
	seperatedMessages := parse(dataString)
	parsedMessage, err := toStruct(seperatedMessages, dataMessage.Received)

	// TODO: need to validate to make sure it has a id
	// if it doesn't return nil

	return &parsedMessage, err
}

func parse(message string) []string {
	startPos := 0
	inBrackets := false
	inQuotes := false

	seperatedMessages := make([]string, 0)
	var currentPos int

	for currentPos = 0; currentPos < len(message); currentPos++ {
		char := message[currentPos]

		if char == ' ' {
			if inQuotes || inBrackets {
				continue
			}
			seperatedMessages = append(seperatedMessages, message[startPos:currentPos])
			startPos = currentPos
		}

		if char == '[' {
			inBrackets = true
		}

		if char == ']' {
			inBrackets = false
		}

		if char == '\'' {
			inQuotes = !inQuotes
		}

	}

	seperatedMessages = append(seperatedMessages, message[startPos:])
	return seperatedMessages
}

func getParams(compRegEx *regexp.Regexp, url string) (paramsMap map[string]string) {

	match := compRegEx.FindStringSubmatch(url)
	paramsMap = make(map[string]string)
	for i, name := range compRegEx.SubexpNames() {
		if i > 0 && i <= len(match) {
			paramsMap[name] = match[i]
		}
	}
	return
}

func toStruct(seperatedMessages []string, receivedTime time.Time) (ParsedMessage, error) {

	if len(seperatedMessages) < 3 {
		return ParsedMessage{}, errors.New("could not parse message")
	}

	tablePreamble, table, op := cleanString(seperatedMessages[0]), cleanString(seperatedMessages[1]), cleanString(seperatedMessages[2])

	if tablePreamble != "table" {
		return ParsedMessage{}, errors.New("could not parse message")
	}

	if !stringInSlice(op, []string{"INSERT", "UPDATE", "DELETE"}) {
		return ParsedMessage{}, errors.New("could not parse message")
	}

	parsedMessage := ParsedMessage{
		Table:    table,
		Op:       op,
		Data:     make(map[string]DataCol),
		Received: receivedTime,
	}

	re := regexp.MustCompile(`(?P<name>.+)\[(?P<type>.+)\]:(?P<value>.+)`)
	for _, value := range seperatedMessages[3:] {
		params := getParams(re, value)

		name, ok := params["name"]
		if !ok {
			log.Printf("Could not parse name")
			return ParsedMessage{}, errors.New("could not parse name")
		}

		type_, ok := params["type"]
		if !ok {
			log.Printf("Could not parse data")
			return ParsedMessage{}, errors.New("could not parse type")
		}

		value, ok := params["value"]
		if !ok {
			fmt.Printf("Could not parse data")
			return ParsedMessage{}, errors.New("could not parse value")
		}

		name = cleanString(name)
		type_ = cleanString(type_)
		value = cleanString(value)

		data := DataCol{
			Name:  name,
			Type:  type_,
			Value: value,
		}

		parsedMessage.Data[name] = data
	}
	return parsedMessage, nil
}
