package reader

import (
	"bytes"
	"testing"
)

func TestKafkaReader(t *testing.T) {
	buffer := &bytes.Buffer{}
	NewReader(buffer)

	got := buffer.String()
	want := "msg-c\nmsg-d\n"

	assertRead(t, got, want)
}

func assertRead(t testing.TB, got, want string) {
	t.Helper()

	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
