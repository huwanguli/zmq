package mq

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// ============================================================
// 优雅关闭测试
// ============================================================

// 辅助函数：启动带优雅关闭的服务器
func startGracefulServer(t *testing.T, port int) *Server {
	srv := NewServer(fmt.Sprintf(":%d", port))
	go srv.Start()
	time.Sleep(100 * time.Millisecond)
	return srv
}

// 关闭后不再接受新连接
func TestGracefulShutdownNoNewConnections(t *testing.T) {
	srv := startGracefulServer(t, 19200)

	// 先验证可以连接
	conn, err := net.Dial("tcp", "localhost:19200")
	if err != nil {
		t.Fatal("关闭前应该能连接")
	}
	conn.Close()

	// 关闭服务器
	srv.Shutdown(2 * time.Second)
	time.Sleep(200 * time.Millisecond)

	// 关闭后应该无法连接
	_, err = net.Dial("tcp", "localhost:19200")
	if err == nil {
		t.Error("关闭后不应该能连接")
	}
}

// 关闭时已有的订阅 goroutine 应该退出
func TestGracefulShutdownCancelsSubscriptions(t *testing.T) {
	srv := startGracefulServer(t, 19201)
	srv.broker.DeclareQueue("q")
	srv.broker.queues["q"].Push(NewMessage("msg1", nil))

	// 消费者连接并订阅
	conn, _ := net.Dial("tcp", "localhost:19201")
	conn.Write(Encode(CmdSubscribe, "q"))
	Decode(conn) // 读 OK

	// 关闭服务器
	srv.Shutdown(2 * time.Second)

	// 消费者应该收到连接错误（因为服务器关闭了连接）
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := Decode(conn)
	if err == nil {
		t.Error("服务器关闭后，消费者应该收到错误")
	}
	conn.Close()
}

// 关闭后正在处理的请求应该完成
func TestGracefulShutdownCompletesPendingRequests(t *testing.T) {
	srv := startGracefulServer(t, 19202)
	srv.broker.DeclareExchange("ex", ExchangeTypeDirect)
	srv.broker.DeclareQueue("q")
	srv.broker.Bind("ex", "q", "key")

	// 发送一个 PUBLISH 请求
	conn, _ := net.Dial("tcp", "localhost:19202")
	conn.Write(Encode(CmdPublish, "ex\nkey\nlast-msg"))

	// 立即关闭服务器
	go srv.Shutdown(2 * time.Second)

	// 应该能收到 OK 响应（请求被处理完了）
	cmd, _, err := Decode(conn)
	if err != nil {
		t.Fatalf("应该收到响应: %v", err)
	}
	if cmd != CmdOK {
		t.Errorf("期望 OK, 实际 %d", cmd)
	}
	conn.Close()
}

// 多次调用 Shutdown 不应该 panic
func TestGracefulShutdownIdempotent(t *testing.T) {
	srv := startGracefulServer(t, 19203)

	srv.Shutdown(2 * time.Second)
	srv.Shutdown(2 * time.Second) // 第二次调用不应该 panic
}
