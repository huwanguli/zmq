package mq

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// ============================================================
// Server 测试 —— 覆盖完整流程 + 错误情况 + 连接断开
// ============================================================

// ---- 辅助函数 ----

func startTestServer(t *testing.T, port int) *Server {
	srv := NewServer(fmt.Sprintf(":%d", port))
	go srv.Start()
	time.Sleep(100 * time.Millisecond)
	return srv
}

func dialAndSend(t *testing.T, port int, frame []byte) (uint32, string) {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.Write(frame)
	cmd, body, err := Decode(conn)
	if err != nil {
		t.Fatal(err)
	}
	return cmd, body
}

// ---- PUBLISH → SUBSCRIBE 完整流程 ----

func TestServerPublishAndSubscribe(t *testing.T) {
	srv := startTestServer(t, 19001)
	srv.broker.DeclareExchange("ex", ExchangeTypeDirect)
	srv.broker.DeclareQueue("q")
	srv.broker.Bind("ex", "q", "key")

	// 发送 PUBLISH
	body := "ex\nkey\nhello world"
	cmd, _ := dialAndSend(t, 19001, Encode(CmdPublish, body))
	if cmd != CmdOK {
		t.Errorf("期望 OK, 实际 %d", cmd)
	}

	// 验证消息到了队列
	q, _ := srv.broker.Consume("q")
	msg := q.Pop(time.Second)
	if msg == nil || msg.Body != "hello world" {
		t.Errorf("期望 'hello world', 实际 %v", msg)
	}
}

func TestServerSubscribeReceivesMessage(t *testing.T) {
	srv := startTestServer(t, 19002)
	srv.broker.DeclareQueue("q")
	srv.broker.queues["q"].Push(NewMessage("test-msg", nil))

	// 发送 SUBSCRIBE
	conn, _ := net.Dial("tcp", "localhost:19002")
	defer conn.Close()
	conn.Write(Encode(CmdSubscribe, "q"))

	// 先读 OK 响应
	cmd, _, err := Decode(conn)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != CmdOK {
		t.Errorf("期望 OK, 实际 %d", cmd)
	}

	// 再读 DELIVER
	cmd, body, err := Decode(conn)
	if err != nil {
		t.Fatal(err)
	}
	if cmd != CmdDeliver {
		t.Errorf("期望 DELIVER, 实际 %d", cmd)
	}
	if !strings.Contains(body, "test-msg") {
		t.Errorf("期望包含 test-msg, 实际 '%s'", body)
	}
}

// ---- 通过网络声明 + 绑定 ----

func TestServerDeclareAndBind(t *testing.T) {
	srv := startTestServer(t, 19003)

	// 声明交换器
	cmd, _ := dialAndSend(t, 19003, Encode(CmdDeclareExchange, "ex\ndirect"))
	if cmd != CmdOK {
		t.Errorf("DeclareExchange 期望 OK, 实际 %d", cmd)
	}

	// 声明队列
	cmd, _ = dialAndSend(t, 19003, Encode(CmdDeclareQueue, "q"))
	if cmd != CmdOK {
		t.Errorf("DeclareQueue 期望 OK, 实际 %d", cmd)
	}

	// 绑定
	cmd, _ = dialAndSend(t, 19003, Encode(CmdBind, "ex\nq\nkey"))
	if cmd != CmdOK {
		t.Errorf("Bind 期望 OK, 实际 %d", cmd)
	}

	// 验证绑定生效
	if len(srv.broker.exchanges) == 0 {
		t.Error("交换器未创建")
	}
}

// ---- 错误情况 ----

func TestServerPublishNoExchange(t *testing.T) {
	startTestServer(t, 19004)
	cmd, _ := dialAndSend(t, 19004, Encode(CmdPublish, "nonexistent\nkey\nbody"))
	if cmd != CmdError {
		t.Errorf("期望 ERROR, 实际 %d", cmd)
	}
}

func TestServerUnknownCommand(t *testing.T) {
	startTestServer(t, 19005)
	cmd, _ := dialAndSend(t, 19005, Encode(99, "body"))
	if cmd != CmdError {
		t.Errorf("期望 ERROR, 实际 %d", cmd)
	}
}

// ---- 连接断开 ----

func TestServerClientDisconnect(t *testing.T) {
	srv := startTestServer(t, 19006)
	srv.broker.DeclareQueue("q")

	conn, _ := net.Dial("tcp", "localhost:19006")
	conn.Write(Encode(CmdSubscribe, "q"))
	time.Sleep(50 * time.Millisecond)
	conn.Close()

	// 服务端不应该崩溃
	time.Sleep(200 * time.Millisecond)
}

// ---- 大消息体 ----

func TestServerLargeBody(t *testing.T) {
	srv := startTestServer(t, 19007)
	srv.broker.DeclareExchange("ex", ExchangeTypeDirect)
	srv.broker.DeclareQueue("q")
	srv.broker.Bind("ex", "q", "key")

	// 10KB 的消息体
	bigBody := strings.Repeat("x", 10240)
	body := fmt.Sprintf("ex\nkey\n%s", bigBody)
	cmd, _ := dialAndSend(t, 19007, Encode(CmdPublish, body))
	if cmd != CmdOK {
		t.Errorf("大消息体发送失败, cmd=%d", cmd)
	}

	q, _ := srv.broker.Consume("q")
	msg := q.Pop(time.Second)
	if msg == nil || len(msg.Body) != 10240 {
		t.Errorf("大消息体接收失败")
	}
}

// ---- 并发连接 ----

func TestServerConcurrentConnections(t *testing.T) {
	srv := startTestServer(t, 19008)
	srv.broker.DeclareExchange("ex", ExchangeTypeDirect)
	srv.broker.DeclareQueue("q")
	srv.broker.Bind("ex", "q", "key")

	for i := 0; i < 20; i++ {
		go func() {
			conn, _ := net.Dial("tcp", "localhost:19008")
			defer conn.Close()
			conn.Write(Encode(CmdPublish, "ex\nkey\nmsg"))
		}()
	}
	time.Sleep(500 * time.Millisecond)

	q, _ := srv.broker.Consume("q")
	if q.Len() != 20 {
		t.Errorf("期望 20 条消息, 实际 %d", q.Len())
	}
}

// ---- io.EOF 测试 ----

func TestServerEOF(t *testing.T) {
	startTestServer(t, 19009)
	conn, _ := net.Dial("tcp", "localhost:19009")
	conn.Close()
	time.Sleep(100 * time.Millisecond)
}
