# Step 2: Queue（队列）

## 什么是队列？

队列是存储消息的容器，**先进先出（FIFO）**。生活类比：排队取餐，先排到的先拿到。

## 设计决策：队列的核心操作

### Pop 的阻塞策略

消费者取消息时，如果队列为空怎么办？

| 方案 | 行为 | 适用场景 |
|------|------|---------|
| A: 返回 nil | 立即返回空 | 轮询模式，浪费 CPU |
| B: 阻塞等待 | 没消息就卡住，有消息立即唤醒 | RabbitMQ 推荐方式 |
| C: 带超时阻塞 | 等一会儿，超时返回 nil | 兼顾阻塞和资源释放 |

**我们选择方案 C（带超时阻塞）**，原因：
- 方案 A 轮询浪费 CPU
- 方案 B 无限阻塞，连接断开时 goroutine 泄漏
- 方案 C 最健壮：超时后可以检查连接状态，决定继续等还是退出

**RabbitMQ 的做法：** 消费者订阅后，Broker 推送消息（长连接）。如果队列为空，连接保持，等待新消息。本质也是阻塞等待，但通过 TCP 连接的心跳检测来处理超时。

### 并发安全方案

多个 goroutine 同时 Push/Pop，如何保证安全？

| 方案 | 优点 | 缺点 |
|------|------|------|
| sync.Mutex | 简单，所有操作互斥 | 并发度低 |
| sync.RWMutex | 读操作并行 | Pop 不是纯读，没用 |
| channel | 天然并发安全，自带阻塞 | 容量固定，不够灵活 |
| channel + slice | 灵活容量 + 阻塞等待 | 实现稍复杂 |

**我们选择 channel + slice 组合：**
- slice 存储消息（灵活容量）
- channel 做通知（阻塞等待 + 唤醒）
- Mutex 保护 slice（并发安全）

### 消息确认机制（ACK）

RabbitMQ 的队列支持两种模式：
- **自动确认**：消息取出即删除（简单，可能丢消息）
- **手动确认**：消费者处理完后回 ACK，消息才删除（可靠）

我们先实现自动确认，ACK 在 Step 5 加入。

## RabbitMQ 队列参考

```go
// RabbitMQ 队列核心属性
type Queue struct {
    Name       string
    Messages   []Message
    Consumers  []Consumer
    Durable    bool     // 持久化（重启保留）
    AutoDelete bool     // 无人消费时自动删除
    Exclusive  bool     // 只允许一个连接使用
    MaxLength  int64    // 最大消息数（0=无限）
    MaxLengthBytes int64 // 最大字节数
    TTL        int32    // 队列级 TTL
}
```

我们只实现 Name + Messages，其余是高级功能。

## 你需要实现的

文件：`mq/queue.go`

1. `Queue` 结构体（Name, messages, mu, notify）
2. `NewQueue(name)` 创建队列
3. `Push(msg)` 消息入队（并发安全，通知等待者）
4. `Pop(timeout)` 消息出队（阻塞等待，超时返回 nil）
5. `Len()` 队列长度

## 验证

运行 `go test ./mq/ -run Queue -v -race` 全部通过。
