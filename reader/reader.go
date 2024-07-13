package reader

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
	"time"
)

func NewReader(out io.Writer) interface{} {
	reads := make(chan string, 3)
	kafkaConsumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"group.id":          "someGroup",
		"auto.offset.reset": "earliest",
	}

	c, err := kafka.NewConsumer(kafkaConsumerConfig)
	if err != nil {
		panic(err)
	}

	var rebalanceConsumerB__ kafka.RebalanceCb = nil // TODO rename and understand

	err = c.SubscribeTopics([]string{"someTopic"}, rebalanceConsumerB__)
	if err != nil {
		panic(err)
	}

	go func() {
		for range 12 {
			msg, err := c.ReadMessage(time.Millisecond * 200)
			if err == nil { // No error
				reads <- fmt.Sprintf("message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			} else if !err.(kafka.Error).IsTimeout() {
				// The client will automatically try to recover from all errors.
				// Timeout is not considered an error because it is raised by
				// ReadMessage in absence of messages.
				reads <- fmt.Sprintf("Consumer error: %v (%v)\n", err, msg)
			}
		}
		close(reads)
	}()

	for c := range reads {
		fmt.Fprintf(out, c)
	}

	return nil
}
