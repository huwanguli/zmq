# Step 5: 二进制帧协议

## 为什么需要协议？

Producer/Broker/Consumer 通过 TCP 通信，需要约定一套"语言"互相理解。

## 设计决策：文本协议 vs 二进制协议

| 方案 | 优点 | 缺点 |
|------|------|------|
| 纯文本（如 HTTP） | 可读性强，调试方便 | 消息体有换行会出错 |
| 二进制头+文本体 | 长度声明，支持任意内容 | 需要编解码 |
| 纯二进制（如 AMQP） | 最紧凑，最高效 | 调试不方便 |

**我们选择二进制头+文本体（混合方案）：**
- 头部用二进制（uint32），精确读取
- 体用文本（按 `\n` 分字段），可读可调试
- 消息体是"剩余全部"，支持任意内容

### 为什么不用纯文本？

纯文本按 `\n` 切分，消息体如果有 `\n` 就会被截断。二进制头声明了体长度，读取时精确读 N 字节，不依赖分隔符。

### 为什么不用纯二进制？

体也用二进制需要额外编码/解码（JSON序列化、protobuf等），学习项目没必要。

## 帧格式

```
┌──────────────┬──────────────┬─────────────┐
│ 命令类型(4字节) │ 体长度(4字节) │ 消息体(N字节) │
│   uint32     │    uint32    │   []byte    │
└──────────────┴──────────────┴─────────────┘
```

- 命令类型：标识这条消息是什么（大端序）
- 体长度：消息体的字节数
- 消息体：实际内容，按 `\n` 分隔各字段

## 命令类型

| 常量 | 值 | 体格式 | 说明 |
|------|-----|--------|------|
| DECLARE_EXCHANGE | 1 | `name\ntype` | 声明交换器 |
| DECLARE_QUEUE | 2 | `name` | 声明队列 |
| BIND | 3 | `exchange\nqueue\nroutingKey` | 绑定 |
| UNBIND | 4 | `exchange\nqueue\nroutingKey` | 解绑 |
| PUBLISH | 5 | `exchange\nroutingKey\nbody` | 发布消息 |
| SUBSCRIBE | 6 | `queue` | 订阅队列 |
| DELIVER | 7 | `id\nbody` | 推送消息 |
| ACK | 8 | `id` | 确认消息 |
| NACK | 9 | `id` | 拒绝消息 |
| OK | 10 | 空 | 成功响应 |
| ERROR | 11 | `reason` | 错误响应 |

## 不同命令的体解析方式

每种命令的体格式不同，解析时按 `\n` 分割，但最后一个字段原样保留（不切分）。

```
PUBLISH:   "exchange\nroutingKey\n消息体(原样保留)"
SUBSCRIBE: "queueName"
DELIVER:   "id\n消息体(原样保留)"
BIND:      "exchange\nqueue\nroutingKey"
```

## 你会学到

- `encoding/binary`：二进制编解码
- `io.ReadFull`：精确读取 N 字节（TCP 粘包/半包处理）
- `strings.SplitN`：按分隔符切分但保留最后字段

## 验证

运行 `go test ./mq/ -run Protocol -v -race` 全部通过。
