package main

import (
	"go-kafka/reader"
	"os"
)

func main() {
	reader.NewReader(os.Stdout)
}
