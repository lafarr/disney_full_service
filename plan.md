# Disney Wait Times Analytics System: Complete Architecture Guide

This guide provides a comprehensive explanation of the Disney wait times analytics system architecture using Go, Kafka, Redis, PostgreSQL, and Elasticsearch. Since you're new to Kafka, I'll explain in detail how each component works together, with special focus on the data flow and interactions.

## Table of Contents

1. [System Overview](#system-overview)
2. [Core Components](#core-components)
3. [Detailed Data Flow](#detailed-data-flow)
4. [Key Technologies Explained](#key-technologies-explained)
5. [Component-by-Component Breakdown](#component-by-component-breakdown)
6. [Real-World Scenarios and Examples](#real-world-scenarios-and-examples)
7. [Implementation Considerations](#implementation-considerations)
8. [Monitoring and Maintenance](#monitoring-and-maintenance)
9. [Adapting the Architecture](#adapting-the-architecture)
10. [Conclusion](#conclusion)

## System Overview

At a high level, our Disney wait times system does the following:

1. Collects wait time data from Disney's APIs or unofficial sources
2. Processes this data to add context and insights
3. Stores both real-time and historical data
4. Makes this data available for dashboards and analysis

This is accomplished through an event-driven architecture that prioritizes reliability, scalability, and real-time processing.

## Core Components

Here's a visual representation of our core architecture components:

```
┌───────────────┐     ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│               │     │               │     │               │     │               │
│  Disney API   │────▶│ Go Collector  │────▶│ raw-wait-     │────▶│ Stream        │ 
│  Sources      │     │  Service      │     │ times-topic   │     │ Processor     │───────────────┐                 
│               │     │               │     │               │     │               │               │   
└───────────────┘     └───────────────┘     └───────────────┘     └───────────────┘               │  
┌───────────────┐     ┌───────────────┐     ┌───────────────┐     ┌───────────────┐               │   
│               │     │               │     │               │     │               │               │  
│   Web UI      │◀────│  WebSocket    │◀────│    Redis      │◀────│ Redis         │               │  
│ (Dashboard)   │     │  API Server   │     │  (Real-time)  │     │ Consumer      │               │  
│               │     │               │     │               │     │               │               │  
└───────────────┘     └───────────────┘     └───────────────┘     └───────▲───────┘               │  
                                                                          │                       │  
                                                                          │                       │  
┌───────────────┐     ┌───────────────┐                            ┌───────────────┐              │  
│               │     │               │                            │               │              │  
│  PostgreSQL   │◀────│  PostgreSQL   │◀───────────────────────────│ enriched-wait-│              │  
│ (Historical)  │     │  Consumer     │                            │ times-topic   │ ◀────────────┘        
│               │     │               │                            │               │              
└───────────────┘     └───────────────┘                            └───────────────┘

           Extended Architecture with Elasticsearch (optional)
                                                               
┌───────────────┐     ┌───────────────┐                            ┌───────────────┐
│               │     │               │                            │               │
│ Elasticsearch │◀────│ Elasticsearch │◀───────────────────────────│ enriched-wait-│
│   (Search)    │     │   Consumer    │                            │ times-topic   │
│               │     │               │                            │               │
└───────────────┘     └───────────────┘                            └───────────────┘
```

Let's introduce each component before diving deeper:

1. **Disney API Sources**: External APIs that provide wait time data
2. **Go Collector Service**: Polls the APIs and formats the data as events
3. **Kafka Topics**: Message queues that store events and enable communication
   - **raw-wait-times-topic**: Contains original wait time data
   - **enriched-wait-times-topic**: Contains wait times with added context
4. **Stream Processor**: Enriches raw data with historical context
5. **Redis Consumer**: Updates Redis with the latest state for real-time access
6. **PostgreSQL Consumer**: Stores complete history in PostgreSQL
7. **Elasticsearch Consumer** (optional): Indexes data for advanced searching
8. **Redis**: In-memory database for real-time state
9. **PostgreSQL**: Relational database for historical data
10. **Elasticsearch** (optional): Search engine for complex querying
11. **WebSocket API Server**: Sends real-time updates to the dashboard
12. **Web UI Dashboard**: Displays wait time data to users

## Detailed Data Flow

Now, let's follow the journey of a wait time update through the entire system:

### 1. Data Collection Phase

**Magic Kingdom's Space Mountain wait time changes from 45 to 75 minutes**

```
Disney API → Go Collector Service → raw-wait-times-topic
```

**What happens:**
1. The Go Collector Service polls the ThemeParks API every 5 minutes
2. It detects the wait time change for Space Mountain
3. It creates a structured event with the new wait time data:
   ```json
   {
     "ride_id": "magic-kingdom-space-mountain",
     "wait_time": 75,
     "status": "operating",
     "timestamp": "2025-05-21T15:30:00Z"
   }
   ```
4. It publishes this event to the `raw-wait-times-topic` in Kafka

**Key technical details:**
- The Collector only publishes when wait times change (or at heartbeat intervals)
- It uses a Kafka producer client in Go to publish messages
- The event is serialized to JSON before publishing
- The ride_id is used as the message key for partitioning

### 2. Enrichment Phase

```
raw-wait-times-topic → Stream Processor → enriched-wait-times-topic
```

**What happens:**
1. The Stream Processor (a continuous running service) reads the event from `raw-wait-times-topic`
2. It queries Redis for historical baseline data for this ride/time/day
3. It calculates additional metrics (percent above normal, trends)
4. It creates an enriched event with all this context:
   ```json
   {
     "ride_id": "magic-kingdom-space-mountain",
     "wait_time": 75,
     "status": "operating",
     "timestamp": "2025-05-21T15:30:00Z",
     "historical_avg": 45,
     "percent_above_normal": 67,
     "trend": "increasing"
   }
   ```
5. It publishes this enriched event to the `enriched-wait-times-topic`

**Key technical details:**
- The Stream Processor runs as a continuous service
- It maintains its position in the input topic using consumer group offsets
- It processes one event at a time (no reprocessing of old events)
- Stateful processing allows trend calculation using recent events

### 3. Storage Phase (happens in parallel)

From the `enriched-wait-times-topic`, multiple consumers read the same events:

#### 3a. Real-time State Update

```
enriched-wait-times-topic → Redis Consumer → Redis
```

**What happens:**
1. The Redis Consumer reads the enriched event
2. It updates several Redis data structures:
   - Current state hash:
     ```
     HMSET "ride:magic-kingdom-space-mountain:current" 
         "wait_time" "75" 
         "historical_avg" "45" 
         "percent_above_normal" "67"
         "trend" "increasing"
         "status" "operating"
         "last_updated" "1716318600"
     ```
   - Recent wait times list (for trend calculation):
     ```
     ZADD "ride:magic-kingdom-space-mountain:recent" 1716318600 "75"
     ```
3. It publishes a notification on a Redis pub/sub channel:
   ```
   PUBLISH "wait-time-updates" "{"ride_id":"magic-kingdom-space-mountain",...}"
   ```

**Key technical details:**
- Redis hash structures provide fast field-based access
- Sorted sets (ZADD) maintain time-ordered data with timestamps as scores
- Redis pub/sub enables real-time notifications without polling

#### 3b. Historical Storage

```
enriched-wait-times-topic → PostgreSQL Consumer → PostgreSQL
```

**What happens:**
1. The PostgreSQL Consumer reads the enriched event
2. It inserts a new row into the wait_times table:
   ```sql
   INSERT INTO wait_times (
     ride_id, wait_time, status, timestamp, 
     historical_avg, percent_above_normal, trend
   ) VALUES (
     'magic-kingdom-space-mountain', 75, 'operating', '2025-05-21T15:30:00Z',
     45, 67, 'increasing'
   );
   ```

**Key technical details:**
- Complete history is maintained in PostgreSQL
- Data is stored in a format optimized for historical analysis
- The consumer handles PostgreSQL-specific error conditions

#### 3c. Search Indexing (Optional)

```
enriched-wait-times-topic → Elasticsearch Consumer → Elasticsearch
```

**What happens:**
1. The Elasticsearch Consumer reads the enriched event
2. It indexes the event in Elasticsearch with appropriate mappings
3. This enables powerful search and aggregation capabilities

### 4. Real-time Dashboard Update Phase

```
Redis → WebSocket API Server → Web UI
```

**What happens:**
1. The WebSocket API Server subscribes to the Redis pub/sub channel
2. When a notification arrives, it reads the updated state from Redis
3. It pushes this update to all connected dashboard clients via WebSockets
4. The dashboard UI updates to show:
   - The new wait time (75 minutes)
   - A visual indicator showing it's 67% above normal (perhaps in red)
   - An upward trend arrow
   - Any other visualizations based on this data

**Key technical details:**
- WebSockets maintain persistent connections to browsers
- Pub/sub pattern eliminates the need for polling
- Updates appear instantly without page refreshes

### 5. Background Maintenance (periodic)

```
PostgreSQL → Background Jobs → Redis (updated baselines)
```

**What happens:**
1. Daily background maintenance jobs run to:
   - Calculate historical baselines based on past data
   - Update Redis with fresh baseline values
   - Clean up old data as needed
2. These jobs ensure the system has accurate baseline data for comparisons
3. They also prevent unbounded growth of storage requirements

**Key technical details:**
- Scheduled jobs run at regular intervals (daily, weekly)
- They calculate aggregates across historical data
- They update reference data used by the stream processor

## Key Technologies Explained

Since you're new to Kafka, let's dive deeper into the key technologies:

### Kafka Core Concepts

**What is Kafka?**  
Apache Kafka is a distributed event streaming platform that lets you publish, subscribe to, store, and process streams of records in real-time. Think of it as a super-powered message queue that can handle massive scale.

**Key Kafka Components:**

1. **Topics**: Named channels where events are published
   - `raw-wait-times-topic` and `enriched-wait-times-topic` in our system
   - Think of these as dedicated pipelines for specific types of data

2. **Partitions**: Each topic is split into partitions for scalability
   - Events with the same key (e.g., ride_id) go to the same partition
   - Enables parallel processing across multiple consumers

3. **Producers**: Applications that publish events to topics
   - Our Go Collector is a producer to `raw-wait-times-topic`
   - Our Stream Processor is both a consumer and a producer

4. **Consumers**: Applications that read events from topics
   - Our Stream Processor, Redis Consumer, and PostgreSQL Consumer are all consumers
   - Consumers track their position in topics using "offsets"

5. **Consumer Groups**: Sets of consumers that divide up the work
   - Each service has its own consumer group
   - Multiple instances of the same service can share a consumer group for scaling

6. **Brokers**: Kafka servers that store the data and serve clients
   - A cluster of brokers provides reliability and scalability
   - Topics are distributed across the brokers

**How Kafka Works for Us:**

In our architecture, Kafka provides:
- **Decoupling**: Components don't need to know about each other
- **Buffering**: Handles load spikes and component outages
- **Ordering**: Maintains the sequence of wait time changes
- **Scalability**: Allows adding more consumers as load increases
- **Replay**: Enables rebuilding state from the event history

### Redis Core Concepts

Redis provides ultra-fast in-memory data storage with rich data structures:

1. **Hashes**: Store the current state of each ride
   ```
   HMSET "ride:space-mountain:current" "wait_time" "75" "status" "operating"
   ```

2. **Sorted Sets**: Store time-ordered data like recent wait times
   ```
   ZADD "ride:space-mountain:recent" 1716318600 "75"
   ```

3. **Pub/Sub**: Notify subscribers about changes without polling
   ```
   PUBLISH "wait-time-updates" "{ride_id: 'space-mountain', wait_time: 75}"
   ```

### PostgreSQL Core Concepts

PostgreSQL stores our complete historical data:

1. **Tables**: Structured storage for wait time history
   ```sql
   CREATE TABLE wait_times (
     id SERIAL PRIMARY KEY,
     ride_id VARCHAR(100),
     wait_time INTEGER,
     status VARCHAR(50),
     timestamp TIMESTAMP,
     historical_avg FLOAT,
     percent_above_normal FLOAT,
     trend VARCHAR(20)
   );
   ```

2. **Indices**: Optimize query performance
   ```sql
   CREATE INDEX idx_wait_times_ride_timestamp ON wait_times (ride_id, timestamp);
   ```

3. **Partitioning**: Split large tables for better performance
   ```sql
   CREATE TABLE wait_times (
     -- columns
   ) PARTITION BY RANGE (timestamp);
   ```

### Elasticsearch Core Concepts (Optional)

Elasticsearch enables powerful search and analytics:

1. **Indices**: Store and index wait time documents
2. **Mappings**: Define how fields are indexed and analyzed
3. **Queries**: Enable complex searches across wait time data
4. **Aggregations**: Calculate metrics and statistics on the fly

## Component-by-Component Breakdown

Now let's dive deeper into each component's implementation:

### 1. Go Collector Service

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"
    "confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
	"github.com/cubehouse/themeparks-go" // Hypothetical package
)

type WaitTimeEvent struct {
	RideID    string    `json:"ride_id"`
	WaitTime  int       `json:"wait_time"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	ctx := context.Background()
	
	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Create Redis client for storing last known state
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redisClient.Close()

	// Create ThemeParks API client
	apiClient := themeparks.NewClient()

	// Run collection loop
	for {
		// Get all ride data from Disney API
		rides, err := apiClient.GetWaitTimes("magic-kingdom")
		if err != nil {
			log.Printf("Error fetching wait times: %v", err)
			time.Sleep(5 * time.Minute)
			continue
		}

		// Process each ride
		for _, ride := range rides {
			// Get last known wait time from Redis
			lastStateKey := "collector:last:" + ride.ID
			lastWaitTimeStr, _ := redisClient.Get(ctx, lastStateKey).Result()
			lastWaitTime := 0
			if lastWaitTimeStr != "" {
				lastWaitTime, _ = strconv.Atoi(lastWaitTimeStr)
			}

			// Check if wait time changed
			if ride.WaitTime != lastWaitTime {
				// Create event
				event := WaitTimeEvent{
					RideID:    ride.ID,
					WaitTime:  ride.WaitTime,
					Status:    ride.Status,
					Timestamp: time.Now(),
				}

				// Serialize to JSON
				eventJson, err := json.Marshal(event)
				if err != nil {
					log.Printf("Error marshaling event: %v", err)
					continue
				}

				// Publish to Kafka
				rawWaitTimesTopic := "raw-wait-times-topic"
				err = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &rawWaitTimesTopic,
						Partition: kafka.PartitionAny,
					},
					Key:   []byte(ride.ID), // Use ride ID as key for partitioning
					Value: eventJson,
				}, nil)
				if err != nil {
					log.Printf("Error producing message: %v", err)
					continue
				}

				// Update last known state
				redisClient.Set(ctx, lastStateKey, ride.WaitTime, 0)
				
				log.Printf("Published wait time change for %s: %d → %d",
					ride.ID, lastWaitTime, ride.WaitTime)
			}
		}

		// Wait 5 minutes before next poll
		time.Sleep(5 * time.Minute)
	}
}
```

**Key aspects:**
- Polls the API every 5 minutes
- Only publishes when wait times change
- Uses Redis to track last known state
- Uses ride_id as the Kafka message key

### 2. Stream Processor

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
)

type WaitTimeEvent struct {
	RideID    string    `json:"ride_id"`
	WaitTime  int       `json:"wait_time"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type EnrichedWaitTimeEvent struct {
	RideID             string    `json:"ride_id"`
	WaitTime           int       `json:"wait_time"`
	Status             string    `json:"status"`
	Timestamp          time.Time `json:"timestamp"`
	HistoricalAvg      float64   `json:"historical_avg"`
	PercentAboveNormal float64   `json:"percent_above_normal"`
	Trend              string    `json:"trend"`
}

func main() {
	ctx := context.Background()

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "stream-processor",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redisClient.Close()

	// Subscribe to raw wait times topic
	consumer.Subscribe("raw-wait-times-topic", nil)

	// Process messages
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Parse the raw event
		var rawEvent WaitTimeEvent
		if err := json.Unmarshal(msg.Value, &rawEvent); err != nil {
			log.Printf("Error parsing event: %v", err)
			continue
		}

		// Fetch historical baseline from Redis
		dayOfWeek := rawEvent.Timestamp.Weekday().String()
		hour := rawEvent.Timestamp.Hour()
		baselineKey := "baseline:" + rawEvent.RideID + ":" + strings.ToLower(dayOfWeek) + ":" + strconv.Itoa(hour)
		
		historicalAvg, err := redisClient.Get(ctx, baselineKey).Float64()
		if err != nil {
			historicalAvg = 0 // Default if no history
		}

		// Calculate percent above normal
		percentAboveNormal := 0.0
		if historicalAvg > 0 {
			percentAboveNormal = (float64(rawEvent.WaitTime) - historicalAvg) / historicalAvg * 100
		}

		// Calculate trend by looking at recent wait times
		recentKey := "ride:" + rawEvent.RideID + ":recent"
		recent, _ := redisClient.ZRevRange(ctx, recentKey, 0, 4).Result() // Last 5 values
		trend := calculateTrend(recent, rawEvent.WaitTime)

		// Update recent wait times list for future trend calculations
		redisClient.ZAdd(ctx, recentKey, &redis.Z{
			Score:  float64(rawEvent.Timestamp.Unix()),
			Member: strconv.Itoa(rawEvent.WaitTime),
		})
		
		// Create enriched event
		enrichedEvent := EnrichedWaitTimeEvent{
			RideID:             rawEvent.RideID,
			WaitTime:           rawEvent.WaitTime,
			Status:             rawEvent.Status,
			Timestamp:          rawEvent.Timestamp,
			HistoricalAvg:      historicalAvg,
			PercentAboveNormal: percentAboveNormal,
			Trend:              trend,
		}

		// Serialize enriched event
		enrichedJson, err := json.Marshal(enrichedEvent)
		if err != nil {
			log.Printf("Error marshaling enriched event: %v", err)
			continue
		}

		// Publish to enriched topic
		enrichedTopic := "enriched-wait-times-topic"
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &enrichedTopic,
				Partition: kafka.PartitionAny,
			},
			Key:   msg.Key, // Keep the same key
			Value: enrichedJson,
		}, nil)
		if err != nil {
			log.Printf("Error producing enriched message: %v", err)
			continue
		}

		log.Printf("Processed wait time for %s: %d minutes (%s)",
			rawEvent.RideID, rawEvent.WaitTime, trend)
	}
}

func calculateTrend(recentValues []string, currentValue int) string {
	if len(recentValues) < 2 {
		return "stable"
	}

	// Convert recent values to integers
	values := make([]int, len(recentValues))
	for i, v := range recentValues {
		values[i], _ = strconv.Atoi(v)
	}

	// Simple trend calculation
	if currentValue > values[0]*1.1 {
		return "increasing"
	} else if currentValue < values[0]*0.9 {
		return "decreasing"
	}
	return "stable"
}
```

**Key aspects:**
- Continuously processes events from `raw-wait-times-topic`
- Enriches each event with historical context
- Maintains recent wait times in Redis for trend calculation
- Produces enriched events to `enriched-wait-times-topic`

### 3. Redis Consumer

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-redis/redis/v8"
)

type EnrichedWaitTimeEvent struct {
	RideID             string    `json:"ride_id"`
	WaitTime           int       `json:"wait_time"`
	Status             string    `json:"status"`
	Timestamp          time.Time `json:"timestamp"`
	HistoricalAvg      float64   `json:"historical_avg"`
	PercentAboveNormal float64   `json:"percent_above_normal"`
	Trend              string    `json:"trend"`
}

func main() {
	ctx := context.Background()

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "redis-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redisClient.Close()

	// Subscribe to enriched wait times topic
	consumer.Subscribe("enriched-wait-times-topic", nil)

	// Process messages
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Parse the enriched event
		var event EnrichedWaitTimeEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error parsing event: %v", err)
			continue
		}

		// Update current state in Redis
		currentKey := "ride:" + event.RideID + ":current"
		err = redisClient.HSet(ctx, currentKey, map[string]interface{}{
			"wait_time":           event.WaitTime,
			"status":              event.Status,
			"historical_avg":      event.HistoricalAvg,
			"percent_above_normal": event.PercentAboveNormal,
			"trend":               event.Trend,
			"last_updated":        event.Timestamp.Unix(),
		}).Err()
		if err != nil {
			log.Printf("Error updating Redis: %v", err)
			continue
		}

		// Also update park-level data
		// Extract park from ride_id (e.g., "magic-kingdom-space-mountain")
		parts := strings.Split(event.RideID, "-")
		if len(parts) >= 2 {
			parkID := parts[0] + "-" + parts[1] // e.g., "magic-kingdom"
			redisClient.HSet(ctx, "park:"+parkID+":rides", event.RideID, event.WaitTime)
		}

		// Publish notification for real-time updates
		notification, _ := json.Marshal(event)
		redisClient.Publish(ctx, "wait-time-updates", notification)

		log.Printf("Updated Redis for %s: %d minutes (%s)",
			event.RideID, event.WaitTime, event.Trend)
	}
}
```

**Key aspects:**
- Consumes from `enriched-wait-times-topic`
- Updates Redis with the current state
- Publishes notifications for WebSocket updates
- Maintains both ride-level and park-level data

### 4. PostgreSQL Consumer

```go
package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/lib/pq"
)

type EnrichedWaitTimeEvent struct {
	RideID             string    `json:"ride_id"`
	WaitTime           int       `json:"wait_time"`
	Status             string    `json:"status"`
	Timestamp          time.Time `json:"timestamp"`
	HistoricalAvg      float64   `json:"historical_avg"`
	PercentAboveNormal float64   `json:"percent_above_normal"`
	Trend              string    `json:"trend"`
}

func main() {
	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"group.id":           "postgres-consumer",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "true",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", "host=localhost user=postgres password=password dbname=disney_wait_times sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Prepare insert statement
	stmt, err := db.Prepare(`
		INSERT INTO wait_times 
		(ride_id, wait_time, status, timestamp, historical_avg, percent_above_normal, trend)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`)
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Subscribe to enriched wait times topic
	consumer.Subscribe("enriched-wait-times-topic", nil)

	// Process messages
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// Parse the enriched event
		var event EnrichedWaitTimeEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Error parsing event: %v", err)
			continue
		}

		// Insert into PostgreSQL
		_, err = stmt.Exec(
			event.RideID,
			event.WaitTime,
			event.Status,
			event.Timestamp,
			event.HistoricalAvg,
			event.PercentAboveNormal,
			event.Trend,
		)
		if err != nil {
			log.Printf("Error inserting into PostgreSQL: %v", err)
			continue
		}

		log.Printf("Stored wait time for %s in PostgreSQL: %d minutes",
			event.RideID, event.WaitTime)
	}
}
```

**Key aspects:**
- Consumes from `enriched-wait-times-topic`
- Inserts each event into PostgreSQL
- Uses prepared statements for efficiency
- Handles PostgreSQL-specific errors

### 5. WebSocket API Server

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

type WaitTimeUpdate struct {
	RideID             string  `json:"ride_id"`
	WaitTime           int     `json:"wait_time"`
	Status             string  `json:"status"`
	HistoricalAvg      float64 `json:"historical_avg"`
	PercentAboveNormal float64 `json:"percent_above_normal"`
	Trend              string  `json:"trend"`
}

var (
	ctx            = context.Background()
	redisClient    *redis.Client
	upgrader       = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins for this example
		},
	}
	clients    = make(map[*websocket.Conn]bool)
	clientsMux sync.Mutex
)

func main() {
	// Create Redis client
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redisClient.Close()

	// Start Redis subscription for updates
	go subscribeToRedisUpdates()

	// Configure HTTP routes
	http.HandleFunc("/ws", handleWebSocketConnection)
	http.HandleFunc("/api/parks", handleParksRequest)
	http.HandleFunc("/api/rides", handleRidesRequest)

	// Start HTTP server
	log.Println("Starting WebSocket server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

// Handles WebSocket connection requests
func handleWebSocketConnection(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading to WebSocket: %v", err)
		return
	}

	// Register client
	clientsMux.Lock()
	clients[conn] = true
	clientsMux.Unlock()

	// Send initial state
	sendInitialState(conn)

	// Handle disconnection
	go func() {
		defer func() {
			conn.Close()
			clientsMux.Lock()
			delete(clients, conn)
			clientsMux.Unlock()
		}()

		// Keep connection alive and handle client messages
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				// Client disconnected
				break
			}
		}
	}()
}

// Sends initial state to a new client
func sendInitialState(conn *websocket.Conn) {
	// Get all parks
	parks, err := redisClient.Keys(ctx, "park:*:rides").Result()
	if err != nil {
		log.Printf("Error fetching parks: %v", err)
		return
	}

	// For each park, get all rides
	for _, parkKey := range parks {
		// Extract park ID from key
		parkID := strings.TrimPrefix(parkKey, "park:")
		parkID = strings.TrimSuffix(parkID, ":rides")

		// Get all rides for this park
		rides, err := redisClient.HGetAll(ctx, parkKey).Result()
		if err != nil {
			continue
		}

		// For each ride, get full details
		for rideID := range rides {
			rideKey := "ride:" + rideID + ":current"
			rideData, err := redisClient.HGetAll(ctx, rideKey).Result()
			if err != nil {
				continue
			}

			// Convert to update object
			waitTime, _ := strconv.Atoi(rideData["wait_time"])
			historicalAvg, _ := strconv.ParseFloat(rideData["historical_avg"], 64)
			percentAbove, _ := strconv.ParseFloat(rideData["percent_above_normal"], 64)

			update := WaitTimeUpdate{
				RideID:             rideID,
				WaitTime:           waitTime,
				Status:             rideData["status"],
				HistoricalAvg:      historicalAvg,
				PercentAboveNormal: percentAbove,
				Trend:              rideData["trend"],
			}

			// Send to client
			updateJSON, _ := json.Marshal(update)
			conn.WriteMessage(websocket.TextMessage, updateJSON)
		}
	}
}

// Subscribes to Redis updates and broadcasts to all clients
func subscribeToRedisUpdates() {
	pubsub := redisClient.Subscribe(ctx, "wait-time-updates")
	defer pubsub.Close()

	for {
		// Wait for new message
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Error receiving Redis message: %v", err)
			continue
		}

		// Broadcast to all clients
		clientsMux.Lock()
		for client := range clients {
			client.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
		}
		clientsMux.Unlock()
	}
}

// API endpoint for fetching park data
func handleParksRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get all parks
	parks, err := redisClient.Keys(ctx, "park:*:rides").Result()
	if err != nil {
		http.Error(w, "Error fetching parks", http.StatusInternalServerError)
		return
	}

	// Format response
	var response []map[string]interface{}
	for _, parkKey := range parks {
		// Extract park ID
		parkID := strings.TrimPrefix(parkKey, "park:")
		parkID = strings.TrimSuffix(parkID, ":rides")

		// Count rides
		rideCount, _ := redisClient.HLen(ctx, parkKey).Result()

		// Create park object
		park := map[string]interface{}{
			"id":        parkID,
			"ride_count": rideCount,
		}
		response = append(response, park)
	}

	// Send response
	json.NewEncoder(w).Encode(response)
}

// API endpoint for fetching ride data
func handleRidesRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Get park ID from query
	parkID := r.URL.Query().Get("park")
	if parkID == "" {
		http.Error(w, "Missing park parameter", http.StatusBadRequest)
		return
	}

	// Get all rides for this park
	parkKey := "park:" + parkID + ":rides"
	rides, err := redisClient.HGetAll(ctx, parkKey).Result()
	if err != nil {
		http.Error(w, "Error fetching rides", http.StatusInternalServerError)
		return
	}

	// Format response
	var response []WaitTimeUpdate
	for rideID := range rides {
		rideKey := "ride:" + rideID + ":current"
		rideData, err := redisClient.HGetAll(ctx, rideKey).Result()
		if err != nil {
			continue
		}

		// Convert to update object
		waitTime, _ := strconv.Atoi(rideData["wait_time"])
		historicalAvg, _ := strconv.ParseFloat(rideData["historical_avg"], 64)
		percentAbove, _ := strconv.ParseFloat(rideData["percent_above_normal"], 64)

		update := WaitTimeUpdate{
			RideID:             rideID,
			WaitTime:           waitTime,
			Status:             rideData["status"],
			HistoricalAvg:      historicalAvg,
			PercentAboveNormal: percentAbove,
			Trend:              rideData["trend"],
		}
		response = append(response, update)
	}

	// Send response
	json.NewEncoder(w).Encode(response)
}
```

**Key aspects:**
- Maintains WebSocket connections with clients
- Subscribes to Redis pub/sub for real-time updates
- Provides REST API endpoints for initial data loading
- Sends updates to all connected clients

### 6. Background Maintenance Jobs

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
)

func main() {
	ctx := context.Background()

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer redisClient.Close()

	// Connect to PostgreSQL
	db, err := sql.Open("postgres", "host=localhost user=postgres password=password dbname=disney_wait_times sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	// Run maintenance jobs
	for {
		log.Println("Running maintenance jobs...")

		// Update historical baselines
		updateHistoricalBaselines(ctx, db, redisClient)

		// Clean up old data
		cleanupOldData(ctx, db, redisClient)

		// Sleep until next run (daily)
		log.Println("Maintenance complete, sleeping until next run")
		time.Sleep(24 * time.Hour)
	}
}

// Updates historical baselines based on PostgreSQL data
func updateHistoricalBaselines(ctx context.Context, db *sql.DB, redisClient *redis.Client) {
	// Query for average wait times by day/hour
	rows, err := db.Query(`
		SELECT 
			ride_id, 
			EXTRACT(DOW FROM timestamp) as day_of_week,
			EXTRACT(HOUR FROM timestamp) as hour_of_day,
			AVG(wait_time) as avg_wait
		FROM wait_times
		WHERE timestamp > NOW() - INTERVAL '90 days'
		GROUP BY ride_id, day_of_week, hour_of_day
	`)
	if err != nil {
		log.Printf("Error querying baseline data: %v", err)
		return
	}
	defer rows.Close()

	// Process results
	for rows.Next() {
		var rideID string
		var dayOfWeek, hourOfDay int
		var avgWait float64

		err := rows.Scan(&rideID, &dayOfWeek, &hourOfDay, &avgWait)
		if err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Convert day of week to string (0 = Sunday)
		days := []string{"sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"}
		if dayOfWeek < 0 || dayOfWeek >= len(days) {
			continue
		}
		dayStr := days[dayOfWeek]

		// Update Redis baseline
		baselineKey := "baseline:" + rideID + ":" + dayStr + ":" + strconv.Itoa(hourOfDay)
		redisClient.Set(ctx, baselineKey, avgWait, 0)
	}

	log.Println("Historical baselines updated")
}

// Cleans up old data to manage storage
func cleanupOldData(ctx context.Context, db *sql.DB, redisClient *redis.Client) {
	// Clean up old PostgreSQL data (keeping 1 year of history)
	_, err := db.Exec("DELETE FROM wait_times WHERE timestamp < NOW() - INTERVAL '1 year'")
	if err != nil {
		log.Printf("Error cleaning up old PostgreSQL data: %v", err)
	}

	// Trim recent ride time lists in Redis (keeping last 100 entries)
	keys, err := redisClient.Keys(ctx, "ride:*:recent").Result()
	if err != nil {
		log.Printf("Error fetching recent keys: %v", err)
	} else {
		for _, key := range keys {
			redisClient.ZRemRangeByRank(ctx, key, 0, -101) // Keep only the last 100
		}
	}

	log.Println("Old data cleanup complete")
}
```

**Key aspects:**
- Runs daily to maintain the system
- Updates historical baselines based on recent data
- Cleans up old data to manage storage requirements
- Ensures the system remains performant over time

## Real-World Scenarios and Examples

Let's explore how the system handles some real-world scenarios:

### Scenario 1: Normal Wait Time Update

**Event**: Space Mountain wait time changes from 45 to 50 minutes (small change)

1. **Collector** detects change, publishes to `raw-wait-times-topic`
2. **Stream Processor** enriches it:
   - Historical average: 45 minutes
   - Percent above normal: 11%
   - Trend: stable (small change)
3. **Redis Consumer** updates Redis:
   - Current state shows 50 minutes, 11% above normal
   - WebSocket clients get update
4. **PostgreSQL Consumer** stores the event for historical record
5. **Dashboard UI** shows a yellow indicator (slightly above normal)

### Scenario 2: Significant Wait Time Spike

**Event**: Seven Dwarfs Mine Train wait time jumps from 60 to 120 minutes

1. **Collector** detects change, publishes to `raw-wait-times-topic`
2. **Stream Processor** enriches it:
   - Historical average: 65 minutes
   - Percent above normal: 85%
   - Trend: increasing (significant change)
3. **Redis Consumer** updates Redis:
   - Current state shows 120 minutes, 85% above normal
   - WebSocket clients get update
4. **PostgreSQL Consumer** stores the event for historical record
5. **Dashboard UI** shows a red indicator (significantly above normal)
   - Perhaps with an alert or highlight

### Scenario 3: Ride Closure

**Event**: Splash Mountain status changes from "operating" to "closed"

1. **Collector** detects status change, publishes to `raw-wait-times-topic`
2. **Stream Processor** enriches it:
   - Wait time: 0 minutes
   - Status: closed
3. **Redis Consumer** updates Redis:
   - Current state shows closed status
   - WebSocket clients get update
4. **PostgreSQL Consumer** stores the closure event for historical record
5. **Dashboard UI** shows ride as unavailable
   - Potentially recalculates optimal touring plans

### Scenario 4: System Component Failure

**Event**: PostgreSQL database goes down temporarily

1. **PostgreSQL Consumer** fails to connect, logs errors
2. **Kafka** retains all events that couldn't be processed
3. **Rest of system** continues functioning:
   - Real-time dashboard still works via Redis
   - Stream processing continues
   - New wait time updates are still collected
4. **When PostgreSQL recovers**:
   - Consumer reconnects and processes backlog of events
   - No data is lost

## Implementation Considerations

When implementing this system, keep these considerations in mind:

### 1. Scaling the Architecture

As your system grows, you'll need to scale different components:

- **Kafka**: Add more brokers and increase partitions for topics
- **Consumers**: Run multiple instances of each consumer type
- **Redis**: Consider Redis Cluster for larger deployments
- **PostgreSQL**: Implement table partitioning and query optimization

### 2. Error Handling and Resilience

Robust error handling is critical:

- Implement retry logic for transient failures
- Add circuit breakers to handle component outages
- Log errors comprehensively for troubleshooting
- Consider dead-letter queues for events that can't be processed

### 3. Security Considerations

Don't forget security aspects:

- Encrypt sensitive data in transit and at rest
- Implement authentication for API endpoints
- Use secure connection settings for all components
- Follow the principle of least privilege for service accounts

### 4. Monitoring and Observability

Set up monitoring for the entire system:

- Track Kafka consumer lag to detect processing bottlenecks
- Monitor Redis memory usage and performance
- Set up PostgreSQL query performance monitoring
- Implement application-level metrics (events processed, errors, etc.)

## Monitoring and Maintenance

A production system requires ongoing monitoring and maintenance:

### Key Metrics to Monitor

1. **Kafka Metrics**:
   - Consumer lag (how far behind real-time each consumer is)
   - Producer/consumer throughput
   - Topic size and growth rate

2. **Redis Metrics**:
   - Memory usage
   - Command latency
   - Connection count
   - Hit/miss ratio for lookups

3. **PostgreSQL Metrics**:
   - Query performance
   - Table size and growth
   - Index usage
   - Connection count

4. **Application Metrics**:
   - Events processed per second
   - Error rates
   - Processing latency
   - API response times

### Regular Maintenance Tasks

1. **Daily**:
   - Update historical baselines
   - Check error logs
   - Verify consumer lag

2. **Weekly**:
   - Clean up old data
   - Review system performance
   - Check for bottlenecks

3. **Monthly**:
   - Optimize database indices
   - Review Kafka topic configurations
   - Scale components as needed
   - Update baseline calculations

## Adapting the Architecture

This architecture is designed to be modular, so you can adapt it to your specific requirements:

### 1. Scaling Options

- For a smaller deployment, you could run all components on a single server
- For larger scale, each component can be deployed to separate servers
- The Kafka and Redis components can be clustered for high availability

### 2. Alternative Technologies

- If Kafka seems too complex initially, you could start with a simpler message queue like RabbitMQ
- If PostgreSQL performance becomes a bottleneck, consider using TimescaleDB (a PostgreSQL extension optimized for time-series data)
- For visualization, you could connect this backend to tools like Grafana or build a custom React/Vue dashboard

### 3. Disney Data Source Options

- Start with the ThemeParks API or Queue-Times API for real-time data
- Incorporate TouringPlans historical data for baseline calculations
- Consider setting up your own data collection if you need more frequent updates

## Getting Started

If you're ready to start implementing, here's a suggested approach:

1. **Start small** - Implement the data collection and a simple dashboard first
2. **Add complexity gradually** - Add the stream processing and historical analysis later
3. **Test thoroughly** - Validate your wait time predictions against actual park data
4. **Iterate based on feedback** - Refine your algorithms based on real-world performance

## Advanced Features to Consider

Once you have the basic system working, you might consider adding:

### 1. Machine Learning Models

- Train models to predict wait times based on weather, day of week, and other factors
- Implement anomaly detection to identify unusual patterns
- Create personalized recommendations based on user preferences

### 2. Geospatial Analysis

- Map wait times geographically across the park
- Calculate optimal routing between attractions
- Analyze crowd flow patterns

### 3. Comparative Analysis

- Compare wait times across different Disney parks
- Benchmark against Universal Studios or other theme parks
- Track changes in wait patterns over years

### 4. Mobile Notifications

- Alert users when their favorite rides have short waits
- Notify about sudden closures or reopenings
- Send recommendations for alternative rides when waits are long

## Conclusion

This Disney wait times analytics system demonstrates a powerful event-driven architecture using Kafka as its backbone. The design provides:

1. **Real-time insights** through stream processing
2. **Historical analysis** through comprehensive data storage
3. **Scalability** through decoupled components
4. **Resilience** through message persistence and redundancy

By separating data collection, processing, storage, and presentation into distinct components, the system achieves a high degree of flexibility and maintainability. Each component focuses on its specific responsibility, making the overall system more robust and easier to evolve over time.

The combination of Kafka, Redis, PostgreSQL, and Elasticsearch provides a balanced approach that leverages each technology's strengths:
- Kafka for reliable event streaming
- Redis for ultra-fast real-time state
- PostgreSQL for comprehensive historical storage
- Elasticsearch for powerful search and analytics (optional)

This architecture can serve as a template for other real-time analytics systems beyond Disney wait times, demonstrating the power of event-driven design for processing time-series data at scale.

By building on this architecture, you can create a sophisticated system that provides valuable insights for Disney park visitors and demonstrates your skills with modern distributed systems technologies.
