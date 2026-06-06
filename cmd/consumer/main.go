package main

import (
	"fmt"
	"log"
	"minimq/client"
)

func main() {
	c, err := client.NewConsumer("localhost:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	c.DeclareQueue("order_queue")

	fmt.Println("[Consumer] waiting for messages...")
	c.Subscribe("order_queue", func(id int64, body string) {
		fmt.Printf("[Consumer] received: id=%d body=%s\n", id, body)
	})
}
