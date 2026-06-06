package main

import (
	"fmt"
	"log"
	"minimq/client"
	"time"
)

func main() {
	p, err := client.NewProducer("localhost:9000")
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	p.DeclareExchange("order_exchange", "direct")
	p.DeclareQueue("order_queue")
	p.Bind("order_exchange", "order_queue", "order.created")

	for i := 1; i <= 5; i++ {
		body := fmt.Sprintf("order #%d", i)
		if err := p.Publish("order_exchange", "order.created", body); err != nil {
			log.Printf("publish failed: %v", err)
		} else {
			fmt.Printf("[Producer] sent: %s\n", body)
		}
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("[Producer] done")
}
