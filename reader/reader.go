package reader

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"io"
)

func NewReader() *kafka.Consumer {
	const autoOffsetReset = "earliest"
	const groupId = "someGroup"
	const bootstrapServers = "localhost:9094"

	kafkaConsumerConfig := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": autoOffsetReset,
	}

	c, err := kafka.NewConsumer(kafkaConsumerConfig)
	if err != nil {
		panic(err)
	}

	return c
}

func consume(out io.Writer, c *kafka.Consumer) {
	defer c.Close()
	reads := make(chan string, 3)

	go func() {
		for i := range 12 {
			fmt.Printf("loop %d\n", i)
			event := c.Poll(500)
			switch e := event.(type) {
			case *kafka.Message:
				fmt.Printf("found %d:%s\n", e.TopicPartition.Partition, string(e.Value))
				reads <- fmt.Sprintf("%d:%s\n", e.TopicPartition.Partition, string(e.Value))
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
			default:
				fmt.Printf("nil event\n")
				continue
			}
		}
		close(reads)
	}()

	for c := range reads {
		fmt.Fprintf(out, c)
	}
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

func consumerAssign(c *kafka.Consumer, topic string, partition int32, offset kafka.Offset) {
	err := c.Assign([]kafka.TopicPartition{{
		Topic:       &topic,
		Partition:   partition,
		Offset:      offset,
		Metadata:    nil,
		Error:       nil,
		LeaderEpoch: nil,
	}})
	if err != nil {
		panic(err)
	}
}
