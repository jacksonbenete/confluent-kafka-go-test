package reader

import (
	"bytes"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"testing"
)

func TestKafkaReader(t *testing.T) {
	t.Run("read from offset", func(t *testing.T) {
		buffer := &bytes.Buffer{}
		consumer := NewReader()

		consumerAssign(consumer, "someTopic", 1, 6)
		consume(buffer, consumer)

		got := buffer.String()
		want := "1:msg-c\n1:msg-d\n"
		assertRead(t, got, want)
	})

	t.Run("read from beginning", func(t *testing.T) {
		buffer := &bytes.Buffer{}
		consumer := NewReader()

		var rebalanceCallback kafka.RebalanceCb = nil
		//consumerSubscribeTopics(consumer, "someTopic", rebalanceCallback)
		consumerSubscribe(consumer, "someTopic", rebalanceCallback)
		consume(buffer, consumer)

		got := buffer.String()
		want := "msg-a" // TODO BUG consumes many but returns 1 or 0, might need to try telemetry
		assertRead(t, got, want)
	})
}

func assertRead(t testing.TB, got, want string) {
	t.Helper()

	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
