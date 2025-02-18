package sender

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Generate mock using: go run go.uber.org/mock/mockgen -source=sender.go -destination=mock_sender.go -package=sender

var (
	ErrRecoverable = errors.New("recoverable error occurred") // Error for retry logic
)

// KafkaSenderConfig defines the configuration for the Kafka sender.
type KafkaSenderConfig struct {
	BatchSize     int           // Number of messages per batch
	FlushInterval time.Duration // Interval to flush pending messages
}

// KafkaSender is the interface for sending messages to Kafka.
type KafkaSender interface {
	Send(message []byte) error
	SendBatch(messages [][]byte) error
	Close() error
}

// kafkaSender implements the KafkaSender interface.
type kafkaSender struct {
	producer *kafka.Producer // Kafka producer instance
	topic    string          // Kafka topic to send messages
	config   KafkaSenderConfig
	wg       sync.WaitGroup // WaitGroup to manage background goroutines
	done     chan struct{}  // Channel to signal shutdown
}

// NewKafkaSender creates a new KafkaSender instance.
func NewKafkaSender(producer *kafka.Producer, topic string, config KafkaSenderConfig) KafkaSender {
	sender := &kafkaSender{
		producer: producer,
		topic:    topic,
		config:   config,
		done:     make(chan struct{}),
	}
	sender.wg.Add(1)
	go sender.processKafkaResponses()
	return sender
}

// Send sends a single message asynchronously without waiting for confirmation.
func (k *kafkaSender) Send(message []byte) error {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
		Value:          message,
	}
	if err := k.producer.Produce(msg, nil); err != nil {
		return classifyError(err)
	}
	return nil
}

// SendBatch sends a batch of messages asynchronously without waiting for confirmation.
func (k *kafkaSender) SendBatch(messages [][]byte) error {
	for _, msg := range messages {
		select {
		case <-k.done:
			return errors.New("Kafka sender is shutting down")
		default:
			err := k.Send(msg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// processKafkaResponses processes Kafka responses asynchronously.
func (k *kafkaSender) processKafkaResponses() {
	defer k.wg.Done()

	for {
		select {
		case <-k.done:
			return
		case event := <-k.producer.Events():
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error != nil {
					_ = classifyError(e.TopicPartition.Error)
				} else {
					// Message delivered successfully
					// Optional: Log successful deliveries
					// fmt.Printf("Message delivered to topic %s [%d] at offset %v\n", *e.TopicPartition.Topic, e.TopicPartition.Partition, e.TopicPartition.Offset)
				}
			case kafka.Error:
				_ = classifyError(e)
				// Optional: Log general errors
				// fmt.Printf("Kafka error: %v\n", e)
			}
		}
	}
}

// Close closes the KafkaSender and ensures all messages are flushed.
func (k *kafkaSender) Close() error {
	close(k.done)
	k.wg.Wait()

	// Ensure all pending messages are flushed
	k.producer.Flush(3000)

	// Close the producer
	k.producer.Close()

	fmt.Println("Kafka producer closed successfully!")
	return nil
}

// classifyError classifies errors to determine if they are recoverable.
func classifyError(err error) error {
	if kafkaErr, ok := err.(kafka.Error); ok {
		switch kafkaErr.Code() {
		case kafka.ErrQueueFull, kafka.ErrTimedOut:
			return ErrRecoverable
		default:
			return kafkaErr
		}
	}
	return err
}
