// Package data hold all contents for retrieving data
package data

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
)

// GetData retrieves the data from @url@, and returns its response as JSON
func GetData[T any](url string) T {
	response, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error making GET request: %v", err)
	}

	defer func() {
		err := response.Body.Close()
		if err != nil {
			log.Fatal(err.Error())
		}
	}()

	if response.StatusCode != http.StatusOK {
		log.Fatalf("Request failed with status code: %d", response.StatusCode)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %v", err)
	}

	var res T
	err = json.Unmarshal([]byte(body), &res)
	if err != nil {
		log.Fatal(err.Error())
	}
	return res
}
