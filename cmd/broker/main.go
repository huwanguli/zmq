package main

import (
	"log"
	"minimq/mq"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Broker 启动入口
// 运行方式：go run ./cmd/broker
// 停止方式：Ctrl+C 触发优雅关闭
func main() {
	srv := mq.NewServer(":9000")

	// 捕获系统信号（Ctrl+C / kill）
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 启动服务器（在 goroutine 中，不阻塞）
	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}()

	// 等待信号
	sig := <-sigCh
	log.Printf("received signal: %v", sig)

	// 优雅关闭：最多等 5 秒
	srv.Shutdown(5 * time.Second)
}
