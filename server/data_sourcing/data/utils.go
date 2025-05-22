// Package data hold all contents for retrieving data
package data

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"time"
)

// TimeFormat represents either 12-hour time or 24-hour time
type TimeFormat int

const (
	// TwentyFourHourTime represents 24-hour time
	TwentyFourHourTime TimeFormat = iota

	// TwelveHourTime represents 12-hour time
	TwelveHourTime
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

// ConvertUTCToET converts a given time in UTC to ET, w/ either a 12-hour
// format or 24-hour format, specified by @format@
// Returns a string represent @dateTime@ in ET in the specified format
func ConvertUTCToET(date string, format TimeFormat) string {
	tUTC, err := time.Parse(time.RFC3339, date)
	if err != nil {
		log.Fatal(err.Error())
	}

	estLocation, err := time.LoadLocation("America/New_York")
	if err != nil {
		log.Fatalf("Error loading Eastern Time zone location: %v", err)
	}

	tEST := tUTC.In(estLocation)

	var customFormat string

	if format == TwelveHourTime {
		customFormat = "01/02/2006 03:04:05 PM"
	} else {
		customFormat = "01/02/2006 15:04:05"
	}

	formattedTimeEST := tEST.Format(customFormat)
	return formattedTimeEST
}
