# Step 3: Exchange（交换器）

## 什么是交换器？

交换器是消息的**路由中心**。Producer 不直接把消息发给 Queue，而是发给 Exchange，由 Exchange 决定消息该去哪些 Queue。

```
Producer → Exchange → [Queue1, Queue2, Queue3]
```

## 设计决策：为什么用接口？

RabbitMQ 有多种 Exchange 类型，路由逻辑不同：

| 类型 | 路由规则 | 典型用途 |
|------|---------|---------|
| Direct | routing key 精确匹配 | 点对点通信 |
| Fanout | 广播到所有绑定的队列 | 发布/订阅 |
| Topic | 通配符匹配（`*` 匹配一个词，`#` 匹配多个词） | 灵活路由 |
| Headers | 按消息头匹配 | 复杂条件路由 |

它们做**同一件事（路由），但做法不同** → 接口的典型场景。

```go
// 接口定义"能做什么"
type Exchange interface {
    Route(routingKey string, msg *Message) []string
}
```

### 为什么 Fanout 也需要 routingKey？

RabbitMQ 的 Fanout Exchange 忽略 routingKey，直接广播。但我们仍然在接口里保留 routingKey 参数，因为：
1. 接口要统一，所有 Exchange 类型用同一个方法签名
2. Fanout 实现里忽略 routingKey 就行
3. 方便后续扩展（比如 Fanout 也可以按 routingKey 过滤）

### Topic Exchange 的通配符规则

```
routingKey 格式：word1.word2.word3（用 . 分隔）

* 匹配一个词：  order.*  → order.created, order.cancelled
# 匹配多个词：  order.#  → order.created, order.created.v2, order
               #.created → order.created, user.created, a.b.created
```

**对比其他技术：**
- RabbitMQ：完整支持 Topic Exchange
- Kafka：用 Consumer Group 实现类似功能
- Redis Pub/Sub：不支持通配符，只能精确匹配 channel

## 你需要实现的

文件：`mq/exchange.go`

1. `Exchange` 接口（Route 方法）
2. `Binding` 结构体（RoutingKey + QueueName）
3. `DirectExchange`（精确匹配）
4. `FanoutExchange`（忽略 routingKey，广播所有）
5. `TopicExchange`（通配符匹配）

## 验证

运行 `go test ./mq/ -run Exchange -v -race` 全部通过。
