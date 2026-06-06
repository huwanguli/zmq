package mq

import (
	"sync"
	"testing"
	"time"
)

// ============================================================
// Broker 测试 —— 覆盖完整流程 + 错误情况 + 并发
// ============================================================

// ---- 基本流程 ----

func TestBrokerPublishDirect(t *testing.T) {
	b := NewBroker()
	b.DeclareExchange("order_ex", ExchangeTypeDirect)
	b.DeclareQueue("order_q")
	b.Bind("order_ex", "order_q", "order.created")

	msg := NewMessage("new order", nil)
	b.Publish("order_ex", "order.created", msg)

	q, _ := b.Consume("order_q")
	got := q.Pop(time.Second)
	if got == nil || got.Body != "new order" {
		t.Errorf("期望 'new order', 实际 %v", got)
	}
}

func TestBrokerPublishFanout(t *testing.T) {
	b := NewBroker()
	b.DeclareExchange("broadcast", ExchangeTypeFanout)
	b.DeclareQueue("q1")
	b.DeclareQueue("q2")
	b.Bind("broadcast", "q1", "")
	b.Bind("broadcast", "q2", "")

	msg := NewMessage("hello all", nil)
	b.Publish("broadcast", "any-key", msg)

	q1, _ := b.Consume("q1")
	q2, _ := b.Consume("q2")
	if q1.Pop(time.Second) == nil {
		t.Error("q1 应该收到消息")
	}
	if q2.Pop(time.Second) == nil {
		t.Error("q2 应该收到消息")
	}
}

func TestBrokerPublishTopic(t *testing.T) {
	b := NewBroker()
	b.DeclareExchange("topic_ex", ExchangeTypeTopic)
	b.DeclareQueue("q1")
	b.DeclareQueue("q2")
	b.Bind("topic_ex", "q1", "order.*")
	b.Bind("topic_ex", "q2", "order.#")

	msg := NewMessage("order created", nil)
	b.Publish("topic_ex", "order.created", msg)

	q1, _ := b.Consume("q1")
	q2, _ := b.Consume("q2")
	if q1.Pop(time.Second) == nil {
		t.Error("q1 (order.*) 应该匹配 order.created")
	}
	if q2.Pop(time.Second) == nil {
		t.Error("q2 (order.#) 应该匹配 order.created")
	}
}

// ---- 错误情况 ----

func TestBrokerPublishNoExchange(t *testing.T) {
	b := NewBroker()
	msg := NewMessage("test", nil)
	err := b.Publish("nonexistent", "key", msg)
	if err == nil {
		t.Error("发布到不存在的交换器应该返回错误")
	}
}

func TestBrokerConsumeNoQueue(t *testing.T) {
	b := NewBroker()
	_, err := b.Consume("nonexistent")
	if err == nil {
		t.Error("消费不存在的队列应该返回错误")
	}
}

func TestBrokerBindNoExchange(t *testing.T) {
	b := NewBroker()
	b.DeclareQueue("q")
	err := b.Bind("nonexistent", "q", "key")
	if err == nil {
		t.Error("绑定不存在的交换器应该返回错误")
	}
}

func TestBrokerBindNoQueue(t *testing.T) {
	b := NewBroker()
	b.DeclareExchange("ex", ExchangeTypeDirect)
	err := b.Bind("ex", "nonexistent", "key")
	if err == nil {
		t.Error("绑定不存在的队列应该返回错误")
	}
}

// ---- 并发测试 ----

func TestBrokerConcurrentPublish(t *testing.T) {
	b := NewBroker()
	b.DeclareExchange("ex", ExchangeTypeDirect)
	b.DeclareQueue("q")
	b.Bind("ex", "q", "key")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := NewMessage("concurrent", nil)
			b.Publish("ex", "key", msg)
		}()
	}
	wg.Wait()

	q, _ := b.Consume("q")
	if q.Len() != 100 {
		t.Errorf("期望 100 条消息, 实际 %d", q.Len())
	}
}

func TestBrokerConcurrentDeclare(t *testing.T) {
	b := NewBroker()
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			b.DeclareQueue("q")
		}()
		go func() {
			defer wg.Done()
			b.DeclareExchange("ex", ExchangeTypeDirect)
		}()
	}
	wg.Wait()
}
