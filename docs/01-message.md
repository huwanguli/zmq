# Step 1: Message（消息）

## 什么是消息？

消息是 MQ 中传递的最小单位，就像快递包裹——里面是实际内容（Body），外面贴着快递单（元数据）。

## 设计决策：消息应该包含哪些字段？

### 方案对比

| 方案 | 字段 | 优点 | 缺点 |
|------|------|------|------|
| 最简 | ID + Body | 简单 | 无法做 TTL、ACK、路由扩展 |
| 标准 | ID + Body + Timestamp + Headers | 功能完整 | 稍复杂 |
| 完整 | + ContentType + CorrelationID + ReplyTo | 类似 AMQP | 过度设计 |

**我们选择"标准"方案**，覆盖核心功能，不过度设计。

### 各字段设计理由

| 字段 | 为什么需要 | RabbitMQ 怎么做的 |
|------|-----------|------------------|
| `ID` | ACK 确认时标识"处理的是哪条消息"，重投递时去重 | AMQP 的 `MessageId`，也可以自动生成 |
| `Body` | 实际要传递的内容 | AMQP 的 `Body`，字节流 |
| `Timestamp` | TTL 超时判断（消息是否过期）、延迟队列 | AMQP 的 `Timestamp`，精确到秒 |
| `Headers` | 自定义元数据（重试次数、来源服务）、Header Exchange 路由 | AMQP 的 `Headers`，键值对表 |

### 为什么 Headers 用 `map[string]string`？

```
方案A: map[string]string     ← 我们选这个
方案B: map[string]interface{}
方案C: []byte（原始字节）
```

**理由：**
- `map[string]string` 简单，够用，类型安全
- `map[string]interface{}` 更灵活但需要类型断言，增加复杂度
- `[]byte` 需要额外编码/解码

RabbitMQ 的 AMQP 协议用的是 `Table` 类型（类似 `map[string]interface{}`），但对于学习项目，`map[string]string` 更合适。

### 为什么用 atomic 生成 ID？

```
方案A: atomic.AddInt64      ← 我们选这个
方案B: sync.Mutex 保护计数器
方案C: UUID
方案D: 数据库自增
```

**理由：**
- `atomic` 是硬件级原子操作，不需要加锁，性能最好
- `Mutex` 也能用，但对单个变量操作来说过于重量级
- `UUID` 全局唯一但无序，不方便调试（看不出来是第几条消息）
- `数据库自增` 需要外部依赖

RabbitMQ 内部用的是 UUID，但我们用递增 ID 更直观，方便学习时观察"第几条消息"。

## RabbitMQ 消息结构参考

```go
// AMQP 0-9-1 消息属性
type Properties struct {
    ContentType     string    // 内容类型，如 "application/json"
    ContentEncoding string    // 内容编码，如 "utf-8"
    Headers         Table     // 自定义键值对（map[string]interface{}）
    DeliveryMode    uint8     // 持久化标记（1=非持久，2=持久）
    Priority        uint8     // 优先级（0-9）
    CorrelationId   string    // 关联 ID（用于 RPC）
    ReplyTo         string    // 回复队列（用于 RPC）
    Expiration      string    // 消息 TTL，如 "60000"（毫秒）
    MessageId       string    // 消息 ID
    Timestamp       time.Time // 创建时间
    Type            string    // 消息类型
    UserId          string    // 用户 ID
    AppId           string    // 应用 ID
}
```

我们只实现了核心子集（ID、Body、Timestamp、Headers），其余是高级功能。

## 你需要实现的

文件：`mq/message.go`

1. `Message` 结构体（ID, Body, Timestamp, Headers）
2. 全局 ID 计数器（atomic 并发安全）
3. `NewMessage(body, headers)` 创建消息
4. `GetHeader(key)` 获取自定义头
5. `SetHeader(key, value)` 设置自定义头

## 验证

运行 `go test ./mq/ -run Message -v` 全部通过。
