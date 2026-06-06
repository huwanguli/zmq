# Step 4: Broker（消息代理）

## Broker 是什么？

Broker 是整个系统的**核心调度中心**，负责：
1. 管理 Exchange 和 Queue
2. 接收 Producer 的消息
3. 通过 Exchange 路由到正确的 Queue
4. 把消息分发给 Consumer

## 设计决策：并发安全方案

多个客户端同时操作 Broker（声明、绑定、发布、消费），如何保证安全？

| 方案 | 优点 | 缺点 |
|------|------|------|
| 一把大锁 | 简单 | 所有操作互斥，并发度低 |
| 每个 map 单独锁 | queues 和 exchanges 操作互不阻塞 | 需要小心锁顺序 |
| sync.Map | 并发安全 | API 不直观，学习价值低 |

**我们选择每个 map 单独锁（RWMutex）：**
- DeclareQueue/DeclareExchange 是写操作（用 Lock）
- Publish 需要读 exchanges + 写 queues（用 RLock + Lock）
- Consume 需要读 queues（用 RLock）

### 为什么 Publish 需要写 queues？

Publish 内部会调用 `queue.Push(msg)`，Push 会修改 messages 切片。但 Push 内部有自己的锁保护，所以 Publish 只需要 RLock 读取 queues map，不需要 Lock。

## 消息流转过程

```
Producer → Broker.Publish(exchange, routingKey, body)
    │
    ▼
找到 Exchange
    │
    ▼
Exchange.Route(routingKey) → ["order_queue", "audit_queue"]
    │
    ▼
对每个匹配的 Queue 调用 Push(msg)
    │
    ▼
Queue 等待 Consumer 来 Pop
```

## 你需要实现的

文件：`mq/broker.go`

1. `Broker` 结构体（queues, exchanges, 对应的锁）
2. `NewBroker()` 创建实例
3. `DeclareExchange(name, exchangeType)` 声明交换器
4. `DeclareQueue(name)` 声明队列
5. `Bind(exchange, queue, routingKey)` 绑定
6. `Publish(exchange, routingKey, body)` 发布消息
7. `Consume(queue)` 获取队列引用

## 验证

运行 `go test ./mq/ -run Broker -v -race` 全部通过。
