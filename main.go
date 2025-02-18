package main

import (
	"fmt"
	"log"
	"macurandb/go-kafka-playground/sender"
	"math/rand"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Kafka configuration constants
const (
	kafkaBroker   = "localhost:9092" // Change this to your Kafka broker
	kafkaTopic    = "test-topic"
	messageSize   = 256     // Size of each message in bytes
	numMessages   = 1000000 // Total number of messages to send
	batchSize     = 1000    // Number of messages per batch
	flushInterval = 2 * time.Second
)

// Generates a random message of the given size
func generateRandomMessage(size int) []byte {
	message := make([]byte, size)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range message {
		message[i] = charset[rand.Intn(len(charset))]
	}
	return message
}

func main() {
	// Create Kafka producer configuration
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            "localhost:9092",
		"acks":                         "1",
		"retries":                      10,
		"queue.buffering.max.messages": 1000000,
		"queue.buffering.max.kbytes":   512000,
		"batch.num.messages":           5000,
		"linger.ms":                    100,
		"compression.type":             "snappy",
	})

	if err != nil {
		log.Fatalf("❌ Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Initialize Kafka sender with configuration
	senderConfig := sender.KafkaSenderConfig{
		BatchSize:     batchSize,
		FlushInterval: flushInterval,
	}
	kafkaSender := sender.NewKafkaSender(producer, kafkaTopic, senderConfig)

	// Start performance testing
	fmt.Printf("🚀 Starting Kafka performance test: Sending %d messages in batches of %d\n", numMessages, batchSize)

	startTime := time.Now()
	successCount, errorCount := 0, 0
	var wg sync.WaitGroup

	// Send messages in batches
	for i := 0; i < numMessages; i += batchSize {
		wg.Add(1)
		go func(batchID int) {
			defer wg.Done()
			messages := make([][]byte, batchSize)

			for j := 0; j < batchSize; j++ {
				messages[j] = generateRandomMessage(messageSize)
			}

			err := kafkaSender.SendBatch(messages)
			if err != nil {
				log.Printf("⚠️  Batch %d failed: %v", batchID, err)
				errorCount += batchSize
			} else {
				successCount += batchSize
			}
		}(i / batchSize)
	}

	// Wait for all messages to be sent
	wg.Wait()

	// Measure time taken
	duration := time.Since(startTime).Seconds()
	throughputEPS := float64(numMessages) / duration
	bandwidth := (float64(messageSize*numMessages) / duration) / (1024 * 1024) // MB/s

	// Print performance results
	fmt.Println("\n📊 Kafka Performance Results:")
	fmt.Printf("✅ Total Messages Sent: %d\n", numMessages)
	fmt.Printf("✅ Successful Messages: %d\n", successCount)
	fmt.Printf("❌ Failed Messages: %d\n", errorCount)
	fmt.Printf("⏱ Total Duration: %.2f seconds\n", duration)
	fmt.Printf("⚡ Throughput: %.2f events/sec\n", throughputEPS)
	fmt.Printf("📡 Bandwidth: %.2f MB/s\n", bandwidth)

	// Close Kafka sender
	kafkaSender.Close()
	fmt.Println("✅ Kafka test completed successfully!")
}
