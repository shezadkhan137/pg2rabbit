package pg2rabbit

import (
	"time"
)

func Fuzz(data []byte) int {
	message := RawMessage{string(data), time.Now()}
	_, err := DoParse(message)
	if err != nil {
		return 0
	}
	return 1
}
