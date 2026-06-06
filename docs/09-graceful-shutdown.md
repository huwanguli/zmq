# Step 9: 优雅关闭

## 什么是优雅关闭？

收到终止信号（Ctrl+C）时：
1. 停止接受新连接
2. 等待已有连接处理完成
3. 通知所有订阅 goroutine 退出
4. 清理资源，程序退出

**不优雅的关闭：** 直接 `os.Exit(0)`，连接被强制断开，消息可能丢失。

## 设计决策：信号处理

Go 用 `os/signal` 包捕获系统信号：

```go
sigCh := make(chan os.Signal, 1)
signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
<-sigCh  // 阻塞等待信号
```

**SIGINT**：用户按 Ctrl+C
**SIGTERM**：kill 命令发送的终止信号

## 关闭流程

```
收到信号
  │
  ▼
停止 Accept 新连接
  │
  ▼
cancel() 通知所有订阅 goroutine
  │
  ▼
等待 goroutine 退出（带超时）
  │
  ▼
关闭 listener
  │
  ▼
程序退出
```

## 你需要实现的

文件：`mq/server.go`

1. `Start()` 改为可被取消（context 参数）
2. `Shutdown()` 优雅关闭方法
3. 信号处理逻辑

## 验证

运行 `go test ./mq/ -run Graceful -race -timeout 30s` 全部通过。
