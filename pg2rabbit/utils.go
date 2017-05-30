package pg2rabbit

import "strings"

func cleanString(s string) string {
	return strings.Trim(s, ": ")
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
