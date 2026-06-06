package main

import (
	"log"
	"minimq/mq"
)

func main() {
	srv := mq.NewServer(":9000")
	if err := srv.Start(); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
