// Package main is the driver class
package main

import (
	"data_sourcing/data"
	"fmt"
	"strconv"
)

func main() {
	waitTimesMap := data.GetWaitTimes()
	for name, wait := range waitTimesMap {
		fmt.Printf("%s %s\n", name, strconv.Itoa(wait))
	}

	// bytes, err := json.Marshal(waitTimesData)
	// if err != nil {
	// 	log.Fatal(err.Error())
	// }
	// fullLiveData := string(bytes)
}
