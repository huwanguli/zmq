# Step 7: Client SDK

## 设计决策：API 风格

### Producer API

```go
p := client.NewProducer("localhost:9000")
p.DeclareExchange("order_exchange", "direct")
p.DeclareQueue("order_queue")
p.Bind("order_exchange", "order_queue", "order.created")
p.Publish("order_exchange", "order.created", "order #1")
p.Close()
```

### Consumer API

```go
c := client.NewConsumer("localhost:9000")
c.DeclareQueue("order_queue")
c.Subscribe("order_queue", func(id int64, body string) {
    fmt.Printf("收到: %s\n", body)
})
```

### Subscribe 的阻塞 vs 回调

| 方案 | 优点 | 缺点 |
|------|------|------|
| 阻塞返回 | 简单，控制流清晰 | 调用者被阻塞 |
| 回调函数 | 非阻塞，更灵活 | 回调里不能轻易退出 |
| 返回 channel | Go 风格，可 select | 需要管理 channel 生命周期 |

**我们选择回调函数**，因为：
- 和 RabbitMQ 的 go-amqp 客户端风格一致
- 用户不需要自己管理 channel
- 回调里 panic 可以被 recover 捕获

## 你会学到

- 封装：隐藏协议细节，暴露干净 API
- 错误处理：检查响应，转换为 Go error
- 连接管理：Close 清理资源

## 你需要实现的

### client/producer.go
- `NewProducer(addr)`：连接 broker
- `DeclareExchange(name, type)`：声明交换器
- `DeclareQueue(name)`：声明队列
- `Bind(exchange, queue, routingKey)`：绑定
- `Publish(exchange, routingKey, body)`：发送消息
- `Close()`：关闭连接

### client/consumer.go
- `NewConsumer(addr)`：连接 broker
- `DeclareQueue(name)`：声明队列
- `Subscribe(queue, handler)`：订阅队列（阻塞）
- `Close()`：关闭连接

## 验证

开两个终端测试：
```
终端1：go run ./cmd/broker
终端2：go run ./cmd/consumer
终端3：go run ./cmd/producer
```
