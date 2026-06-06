# Step 6: TCP Server

## 设计决策：连接生命周期管理

### 每个连接一个 goroutine

```
主 goroutine: Accept() → 来一个连接 → go handleConnection(conn)
连接 goroutine: 循环 Decode → ParseBody → handleCommand
```

**为什么用 goroutine 而不是线程？**
- goroutine 是 Go 的轻量级线程，初始栈只有几 KB
- 可以轻松创建几万个 goroutine
- 线程（OS 线程）通常需要几 MB 栈空间

### 连接断开处理

```
客户端断开 → conn.Read 返回 io.EOF → goroutine 退出
```

**关键：** SUBSCRIBE 的消费 goroutine 在连接断开后，
`conn.Write(DELIVER)` 会失败，应该退出循环。

### 错误处理策略

| 错误类型 | 处理方式 |
|---------|---------|
| io.EOF | 客户端正常断开，退出循环 |
| Decode 错误 | 打印日志，继续读下一帧 |
| Command 错误 | 发送 ERROR 响应，继续处理 |

## RabbitMQ 对比

RabbitMQ 的连接管理更复杂：
- 心跳检测（60 秒无数据断开）
- Channel 多路复用（一个连接多个逻辑通道）
- SASL 认证
- 连接协商

我们只实现基础的连接生命周期。

## 你需要实现的

文件：`mq/server.go`

1. `Server` 结构体（addr, broker）
2. `NewServer(addr)` 创建实例
3. `Start()` 启动监听（阻塞）
4. `handleConnection(conn)` 处理单个连接
5. `handleCommand(conn, cmd, fields)` 分发命令

## 验证

运行 `go test ./mq/ -run Server -v -race` 全部通过。
