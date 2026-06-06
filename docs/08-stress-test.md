# Step 8: 并发压力测试

## 为什么需要压力测试？

前面的测试都是"正常使用"场景。压力测试模拟**极端情况**：
- 几十个客户端同时连接
- 几百条消息同时发布
- 客户端快速连接/断开
- 生产者和消费者速度不匹配

## 设计决策：测试什么？

### Race Detector（竞态检测器）

Go 内置的 `-race` 标志可以检测数据竞争：
```bash
go test -race ./...
```

原理：运行时监控所有内存访问，如果两个 goroutine 同时读写同一变量且没有同步，报告 race condition。

### 压力场景

| 场景 | 验证目标 |
|------|---------|
| 50 个生产者并发发布 | Broker 并发安全 |
| 50 个消费者并发订阅 | Queue 并发安全 |
| 快速连接/断开 | goroutine 不泄漏 |
| 生产者快、消费者慢 | Queue 不会丢消息 |
| 消费者快、生产者慢 | 阻塞等待正常工作 |
| 大量消息堆积 | 内存不会爆 |

### Goroutine 泄漏检测

用 `runtime.NumGoroutine()` 检测 goroutine 数量：
```go
before := runtime.NumGoroutine()
// 执行操作
after := runtime.NumGoroutine()
if after > before + expected {
    t.Errorf("goroutine 泄漏: before=%d after=%d", before, after)
}
```

## 你需要实现的

文件：`mq/stress_test.go`

1. `TestStressConcurrentPublish`：50 个 goroutine 同时发布
2. `TestStressConcurrentConsume`：50 个 goroutine 同时消费
3. `TestStressRapidConnectDisconnect`：快速连接/断开
4. `TestStressProducerFasterThanConsumer`：生产者比消费者快

## 验证

运行 `go test ./mq/ -run Stress -race -timeout 60s -v` 全部通过。
