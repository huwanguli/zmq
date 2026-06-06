package mq

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// ============================================================
// 并发压力测试 —— 验证系统在极端情况下的健壮性
//
// 所有测试都用 -race 标志运行，检测数据竞争
// ============================================================

// 辅助函数：启动服务器并返回
func startStressServer(t *testing.T, port int) *Server {
	srv := NewServer(fmt.Sprintf(":%d", port))
	go srv.Start()
	time.Sleep(100 * time.Millisecond)
	return srv
}

// 场景1：多个生产者并发发布到同一个队列
func TestStressConcurrentPublish(t *testing.T) {
	srv := startStressServer(t, 19100)
	srv.broker.DeclareExchange("ex", ExchangeTypeDirect)
	srv.broker.DeclareQueue("q")
	srv.broker.Bind("ex", "q", "key")

	var wg sync.WaitGroup
	producerCount := 50
	msgsPerProducer := 10

	for i := 0; i < producerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", "localhost:19100")
			if err != nil {
				t.Errorf("producer %d connect failed: %v", id, err)
				return
			}
			defer conn.Close()
			for j := 0; j < msgsPerProducer; j++ {
				body := fmt.Sprintf("ex\nkey\nmsg-%d-%d", id, j)
				conn.Write(Encode(CmdPublish, body))
				// 读取 OK 响应
				Decode(conn)
			}
		}(i)
	}
	wg.Wait()

	// 验证消息数量
	q, _ := srv.broker.Consume("q")
	expected := producerCount * msgsPerProducer
	if q.Len() != expected {
		t.Errorf("期望 %d 条消息, 实际 %d", expected, q.Len())
	}
}

// 场景2：多个消费者并发订阅同一个队列
func TestStressConcurrentConsume(t *testing.T) {
	srv := startStressServer(t, 19101)
	srv.broker.DeclareQueue("q")

	// 预先放入 100 条消息
	for i := 0; i < 100; i++ {
		srv.broker.queues["q"].Push(NewMessage(fmt.Sprintf("msg-%d", i), nil))
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	totalConsumed := 0
	consumerCount := 10

	for i := 0; i < consumerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", "localhost:19101")
			if err != nil {
				t.Errorf("consumer %d connect failed: %v", id, err)
				return
			}
			defer conn.Close()

			// 发送 SUBSCRIBE
			conn.Write(Encode(CmdSubscribe, "q"))
			Decode(conn) // 读 OK

			// 消费消息
			consumed := 0
			for {
				conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				cmd, _, err := Decode(conn)
				if err != nil {
					break // 超时或连接关闭
				}
				if cmd == CmdDeliver {
					consumed++
				}
			}
			mu.Lock()
			totalConsumed += consumed
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if totalConsumed != 100 {
		t.Errorf("期望消费 100 条消息, 实际 %d", totalConsumed)
	}
}

// 场景3：快速连接/断开，验证 goroutine 不泄漏
func TestStressRapidConnectDisconnect(t *testing.T) {
	srv := startStressServer(t, 19102)
	srv.broker.DeclareQueue("q")

	for i := 0; i < 30; i++ {
		conn, err := net.Dial("tcp", "localhost:19102")
		if err != nil {
			t.Fatal(err)
		}
		// 快速发送 SUBSCRIBE 然后立即断开
		conn.Write(Encode(CmdSubscribe, "q"))
		conn.Close()
	}

	// 等待 goroutine 退出
	time.Sleep(2 * time.Second)

	// 验证服务端没有崩溃
	conn, err := net.Dial("tcp", "localhost:19102")
	if err != nil {
		t.Fatal("服务端已崩溃")
	}
	conn.Write(Encode(CmdDeclareQueue, "test_q"))
	cmd, _, _ := Decode(conn)
	if cmd != CmdOK {
		t.Error("服务端响应异常")
	}
	conn.Close()
}

// 场景4：生产者比消费者快，验证消息不丢失
func TestStressProducerFasterThanConsumer(t *testing.T) {
	srv := startStressServer(t, 19103)
	srv.broker.DeclareExchange("ex", ExchangeTypeDirect)
	srv.broker.DeclareQueue("q")
	srv.broker.Bind("ex", "q", "key")

	// 生产者快速发布 100 条消息
	producerConn, _ := net.Dial("tcp", "localhost:19103")
	for i := 0; i < 100; i++ {
		body := fmt.Sprintf("ex\nkey\nfast-msg-%d", i)
		producerConn.Write(Encode(CmdPublish, body))
		Decode(producerConn) // 读 OK
	}
	producerConn.Close()

	// 消费者慢速消费
	consumerConn, _ := net.Dial("tcp", "localhost:19103")
	consumerConn.Write(Encode(CmdSubscribe, "q"))
	Decode(consumerConn) // 读 OK

	consumed := 0
	for i := 0; i < 100; i++ {
		consumerConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		cmd, body, err := Decode(consumerConn)
		if err != nil {
			t.Fatalf("消费第 %d 条时超时: %v", i, err)
		}
		if cmd != CmdDeliver {
			t.Fatalf("期望 DELIVER, 实际 %d", cmd)
		}
		if body == "" {
			t.Fatal("消息体为空")
		}
		consumed++
		time.Sleep(10 * time.Millisecond) // 慢速消费
	}
	consumerConn.Close()

	if consumed != 100 {
		t.Errorf("期望消费 100 条, 实际 %d", consumed)
	}
}

// 场景5：Fanout 广播压力测试
func TestStressFanoutBroadcast(t *testing.T) {
	srv := startStressServer(t, 19104)
	srv.broker.DeclareExchange("broadcast", ExchangeTypeFanout)
	queueCount := 10

	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("q%d", i)
		srv.broker.DeclareQueue(qName)
		srv.broker.Bind("broadcast", qName, "")
	}

	// 发布 20 条消息
	for i := 0; i < 20; i++ {
		msg := NewMessage(fmt.Sprintf("broadcast-%d", i), nil)
		srv.broker.Publish("broadcast", "", msg)
	}

	// 验证每个队列都有 20 条消息
	for i := 0; i < queueCount; i++ {
		qName := fmt.Sprintf("q%d", i)
		q, _ := srv.broker.Consume(qName)
		if q.Len() != 20 {
			t.Errorf("队列 %s 期望 20 条, 实际 %d", qName, q.Len())
		}
	}
}
