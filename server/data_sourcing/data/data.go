// Package data contains all data retrieval functionality
package data

import (
	"fmt"
	"strings"
)

// Park represents a single park within a destination, e.g. Magic Kingdom, within Disney World
type Park struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Destination represents a single destination - Disney World, Disneyland Tokyo, etc.
type Destination struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Slug  string `json:"slug"`
	Parks []Park `json:"parks"`
}

// DestinationsResponse represents the filtered response from https://api.themeparks.wiki/v1/destinations
type DestinationsResponse struct {
	Destinations []Destination `json:"destinations"`
}

// EntityType represents the type of an entity (destination, park, attraction, restaurant, hotel, show)
type EntityType string

const (
	// EntityTypeDestination represents a destination
	EntityTypeDestination EntityType = "DESTINATION"

	// EntityTypePark represents a park
	EntityTypePark EntityType = "PARK"

	// EntityTypeAttraction represents an attraction
	EntityTypeAttraction EntityType = "ATTRACTION"

	// EntityTypeRestaurant represents a restaurant
	EntityTypeRestaurant EntityType = "RESTAURANT"

	// EntityTypeHotel represents a hotel
	EntityTypeHotel EntityType = "HOTEL"

	// EntityTypeShow represents a show
	EntityTypeShow EntityType = "SHOW"
)

// LiveStatus represents the current status of an entity
type LiveStatus string

const (
	// LiveStatusOperating represens an entity that is operating
	LiveStatusOperating LiveStatus = "OPERATING"

	// LiveStatusDown represens an entity that is down
	LiveStatusDown LiveStatus = "DOWN"

	// LiveStatusClosed represens an entity that is closed
	LiveStatusClosed LiveStatus = "CLOSED"

	// LiveStatusRefurbishment represens an entity that is down due to being refurbished
	LiveStatusRefurbishment LiveStatus = "REFURBISHMENT"
)

// ReturnTimeState represents the state of the return time availability
type ReturnTimeState string

const (
	// ReturnTimeAvailable represents return time being available
	ReturnTimeAvailable ReturnTimeState = "AVAILABLE"

	// ReturnTimeTempFull represents return time being temporarily full
	ReturnTimeTempFull ReturnTimeState = "TEMP_FULL"

	// ReturnTimeFinished represents return time being finished for the day
	ReturnTimeFinished ReturnTimeState = "FINISHED"
)

// Standby represents the data on the standby line of an entity
type Standby struct {
	WaitTime int `json:"waitTime"`
}

// SingleRider represents the data on the single rider line of an entity
type SingleRider struct {
	WaitTime      int `json:"waitTime"`
	DataAvailable bool
}

// ReturnTime represents the data available on the return time "line"
type ReturnTime struct {
	State                ReturnTimeState `json:"state"`
	ReturnStart          string          `json:"returnStart"`
	ReturnEnd            string          `json:"returnEnd"`
	ReturnStartAvailable bool
	ReturnEndAvailable   bool
}

// Price represents the price data of an item, experience, etc.
type Price struct {
	Amount    int    `json:"amount"`
	Currency  string `json:"currency"`
	Formatted string `json:"formatted"`
}

// PaidReturnTime represents the data available on the paid return time "line" (lightning lane / Genie+)
type PaidReturnTime struct {
	State                ReturnTimeState `json:"state"`
	ReturnStart          string          `json:"returnStart"`
	ReturnEnd            string          `json:"returnEnd"`
	ReturnStartAvailable bool
	ReturnEndAvailable   bool
	PriceData            Price `json:"price"`
}

// BoardingGroupState represents the availability of information on the current boarding group
type BoardingGroupState string

const (
	// BoardingGroupStateAvailable represents the boarding group state of a paid return time being available
	BoardingGroupStateAvailable BoardingGroupState = "AVAILABLE"

	// BoardingGroupStatePaused represents the boarding group state of a paid return time being paused
	BoardingGroupStatePaused BoardingGroupState = "PAUSED"

	// BoardingGroupStateClosed represents the boarding group state of a paid return time being closed
	BoardingGroupStateClosed BoardingGroupState = "CLOSED"
)

// BoardingGroup represents the data on the current boarding group for a (paid or unpaid) return time queue
type BoardingGroup struct {
	AllocationStatus            BoardingGroupState `json:"allocationStatus"`
	CurrentGroupStart           int                `json:"currentGroupStart"`
	CurrentGroupEnd             int                `json:"currentGroupEnd"`
	NextAllocationTime          string             `json:"nextAllocationTime"`
	EstimatedWait               int                `json:"estimatedWait"`
	CurrentGroupStartAvailable  bool
	CurrentGroupEndAvailable    bool
	NextAllocationTimeAvailable bool
	EstimatedWaitAvailable      bool
}

// PaidStandBy represents the data on a paid standby line (lightning lane)
type PaidStandBy struct {
	WaitTime          int `json:"waitTime"`
	WaitTimeAvailable bool
}

// LiveQueue represents the live data on a queue
type LiveQueue struct {
	StandbyData        Standby        `json:"STANDBY"`
	SingleRiderData    SingleRider    `json:"SINGLE_RIDER"`
	ReturnTimeData     ReturnTime     `json:"RETURN_TIME"`
	PaidReturnTimeData PaidReturnTime `json:"PAID_RETURN_TIME"`
	BoardingGroupData  BoardingGroup  `json:"BOARDING_GROUP"`
	PaidStandByData    PaidStandBy    `json:"PAID_STANDBY"`
}

// LiveShowTime represents a show time for some entity
type LiveShowTime struct {
	Type               string `json:"type"`
	StartTime          string `json:"startTime"`
	EndTime            string `json:"endTime"`
	StartTimeAvailable bool
	EndTimeAvailable   bool
}

// LiveOperatingHours represents the operating hours for some entity
type LiveOperatingHours struct {
	Type               string `json:"type"`
	StartTime          string `json:"startTime"`
	EndTime            string `json:"endTime"`
	StartTimeAvailable bool
	EndTimeAvailable   bool
}

// LiveDiningAvailability represents the current dining availability for a restaurant
type LiveDiningAvailability struct {
	PartySize          int `json:"partySize"`
	WaitTime           int `json:"waitTime"`
	PartySizeAvailable bool
	WaitTimeAvailable  bool
}

// EntityLiveData represents all live data on an entity
type EntityLiveData struct {
	ID                 string                   `json:"id"`
	Name               string                   `json:"name"`
	Type               EntityType               `json:"entityType"`
	Status             LiveStatus               `json:"status"`
	LastUpdated        string                   `json:"lastUpdated"`
	Queue              LiveQueue                `json:"queue"`
	Showtimes          []LiveShowTime           `json:"showtimes"`
	OperatingHours     []LiveOperatingHours     `json:"operatingHours"`
	DiningAvailability []LiveDiningAvailability `json:"diningAvailability"`
}

// EntityLiveDataResponse represents a response from https://api.themeparks.wiki/v1/entity/{entityID}/live
type EntityLiveDataResponse struct {
	ID       string           `json:"id"`
	Name     string           `json:"name"`
	Type     EntityType       `json:"entityType"`
	Timezone string           `json:"timezone"`
	LiveData []EntityLiveData `json:"liveData"`
}

const destinationsURL = "https://api.themeparks.wiki/v1/destinations"
const waitTimesURLFormat = "https://api.themeparks.wiki/v1/entity/%s/live"

// GetWDWDestination the data for the WDW destination from https://api.themeparks.wiki/v1/destinations
// Returns the respone from @url@ as type data.Destination
func GetWDWDestination() Destination {
	res := GetData[DestinationsResponse](destinationsURL)

	newDestinations := []Destination{}
	for _, dest := range res.Destinations {
		destinationName := strings.ToLower(dest.Name)
		if strings.Contains(destinationName, "walt disney world") {
			newDestinations = append(newDestinations, dest)
		}
	}
	res.Destinations = newDestinations
	return res.Destinations[0]
}

// GetWaitTimes gets the wait times for all entities within a destination
// Returns a map[@name@]@wait time@
func GetWaitTimes() map[string]int {
	wdwID := (GetWDWDestination()).ID
	liveData := GetData[EntityLiveDataResponse](fmt.Sprintf(waitTimesURLFormat, wdwID))
	waitTimes := map[string]int{}
	for _, liveDataEntry := range liveData.LiveData {
		standby := liveDataEntry.Queue.StandbyData
		if standby.WaitTime != 0 {
			waitTimes[liveDataEntry.Name] = standby.WaitTime
		}
	}
	return waitTimes
}
