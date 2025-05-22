// Package main is the driver class
package main

import (
	"data_sourcing/data"
	"fmt"
	"strconv"
)

func main() {
	res := data.GetWaitTimes()
	for _, liveDataEntry := range res.LiveData {
		standby := liveDataEntry.Queue.StandbyData
		if liveDataEntry.Type == data.EntityTypeHotel {
			fmt.Println(liveDataEntry.Name + " " + strconv.Itoa(standby.WaitTime))
		}
	}
}
