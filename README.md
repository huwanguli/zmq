# MiniMQ

用 Go 从零实现一个简化版 RabbitMQ，学习消息队列的核心原理。

## 快速开始

```bash
# 启动 Broker
go run ./cmd/broker

# 新终端：启动消费者
go run ./cmd/consumer

# 新终端：启动生产者
go run ./cmd/producer

# 停止 Broker：Ctrl+C（优雅关闭）
```

## 项目结构

```
minimq/
├── mq/                        # 核心组件
│   ├── message.go             # 消息结构体（ID, Body, Timestamp, Headers）
│   ├── queue.go               # 并发安全队列（Mutex + channel 阻塞）
│   ├── exchange.go            # 交换器（接口 + Direct/Fanout/Topic）
│   ├── broker.go              # 核心引擎（路由 + 双锁并发安全）
│   ├── protocol.go            # 二进制帧协议（11 种命令）
│   └── server.go              # TCP 服务端（context 生命周期管理）
├── client/                    # 客户端 SDK
│   ├── producer.go            # 生产者（封装协议细节）
│   └── consumer.go            # 消费者（回调式消息处理）
├── cmd/                       # 命令行入口
│   ├── broker/main.go         # Broker 启动（信号捕获 + 优雅关闭）
│   ├── producer/main.go       # 生产者 demo
│   └── consumer/main.go       # 消费者 demo
└── docs/                      # 学习文档
```

## 核心架构

```
Producer → [Exchange] → Binding → [Queue] → Consumer
              ↑                      ↑
         路由规则（routing key）   并发安全存储
```

**消息流转：**
1. Producer 发送消息到 Exchange，指定 routing key
2. Exchange 根据 routing key 和 Binding 规则，找到匹配的 Queue
3. 消息 Push 到 Queue
4. Consumer 从 Queue Pop 消息（阻塞等待）

## 学习路线

按顺序阅读文档，每一步都有设计决策和代码实现：

| 步骤 | 文档 | 学习内容 |
|------|------|---------|
| 1 | [Message](docs/01-message.md) | 消息结构体、atomic 并发安全 |
| 2 | [Queue](docs/02-queue.md) | 队列、Mutex、channel 阻塞等待 |
| 3 | [Exchange](docs/03-exchange.md) | 接口设计、Direct/Fanout/Topic 路由 |
| 4 | [Broker](docs/04-broker.md) | 核心引擎、RWMutex 双锁 |
| 5 | [Protocol](docs/05-protocol.md) | 二进制帧协议、io.ReadFull |
| 6 | [Server](docs/06-server.md) | TCP 编程、连接生命周期 |
| 7 | [SDK](docs/07-sdk.md) | 客户端封装、回调式消费 |
| 8 | [Stress Test](docs/08-stress-test.md) | 并发压力测试、race detector |
| 9 | [Graceful Shutdown](docs/09-graceful-shutdown.md) | context、信号处理、优雅关闭 |

## 技术要点

- **并发安全**：Mutex、RWMutex、atomic、channel
- **网络编程**：TCP、二进制协议、io.ReadFull（解决粘包/半包）
- **接口设计**：Exchange 接口 + 三种实现（多态）
- **生命周期管理**：context 控制 goroutine，signal 捕获优雅关闭

## 测试

```bash
# 运行所有测试（含 race detector）
go test ./mq/ -race -timeout 60s

# 运行压力测试
go test ./mq/ -run Stress -race -timeout 60s

# 运行优雅关闭测试
go test ./mq/ -run Graceful -race -timeout 30s
```

## RabbitMQ 对比

| 特性 | RabbitMQ | MiniMQ |
|------|----------|--------|
| 协议 | AMQP 0-9-1 | 自定义二进制 |
| Exchange 类型 | Direct/Fanout/Topic/Headers | Direct/Fanout/Topic |
| 消息确认 | ACK/NACK（手动/自动） | 无（待实现） |
| 持久化 | 磁盘存储 | 内存 |
| 集群 | 支持 | 不支持 |
| Channel 多路复用 | 支持 | 不支持 |
| 死信队列 | 支持 | 不支持 |
| TTL | 支持 | 不支持 |

## 扩展方向

- [ ] ACK/NACK 确认机制
- [ ] 死信队列（DLQ）
- [ ] 消息 TTL
- [ ] 优先级队列
- [ ] 消息持久化
- [ ] Channel 多路复用
- [ ] AMQP 协议兼容
