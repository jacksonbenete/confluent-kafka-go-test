package reader

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
	"time"
)

func NewReader(out io.Writer) interface{} {
	const autoOffsetReset = "earliest"
	const groupId = "someGroup"
	const bootstrapServers = "localhost:9094"
	var topic = "someTopic"

	reads := make(chan string, 3)

	kafkaConsumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": autoOffsetReset,
	}

	c, err := kafka.NewConsumer(kafkaConsumerConfig)
	if err != nil {
		panic(err)
	}
	defer c.Close()

	var rebalanceCallback kafka.RebalanceCb = nil
	consumerSubscribeTopics(c, topic, rebalanceCallback)
	//consumerSubscribe(c, topic, rebalanceCallback)
	//consumerAssign(c, topic)

	go func() {
		for range 12 {
			// ReadMessage is a wrapper around .Poll()
			msg, err := c.ReadMessage(time.Millisecond * 200)
			if err == nil { // No error
				reads <- fmt.Sprintf("%s\n", string(msg.Value))
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

func consumerSubscribeTopics(c *kafka.Consumer, topic string, rebalanceCallback kafka.RebalanceCb) {
	err := c.SubscribeTopics([]string{topic}, rebalanceCallback)
	if err != nil {
		panic(err)
	}
}

func consumerSubscribe(c *kafka.Consumer, topic string, rebalanceCallback kafka.RebalanceCb) {
	err := c.Subscribe(topic, rebalanceCallback)
	if err != nil {
		panic(err)
	}
}

func consumerAssign(c *kafka.Consumer, topic string) {
	err := c.Assign([]kafka.TopicPartition{{
		Topic:       &topic,
		Partition:   1,
		Offset:      6,
		Metadata:    nil,
		Error:       nil,
		LeaderEpoch: nil,
	}})
	if err != nil {
		panic(err)
	}
}
