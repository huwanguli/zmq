package mq

// ============================================================
// Message —— MQ 中传递的最小单位
//
// RabbitMQ 的消息包含 Properties（元数据）和 Body（内容）
// 我们实现核心字段：ID、Body、Timestamp、Headers
// ============================================================

import (
	"sync/atomic"
	"time"
)

// 全局消息 ID 计数器，atomic 保证并发安全
var msgID int64

// Message 消息结构体
type Message struct {
	ID        int64             // 唯一标识，用于 ACK 确认
	Body      string            // 消息体（实际内容）
	Timestamp time.Time         // 创建时间，用于 TTL 超时
	Headers   map[string]string // 自定义键值对，用于消息过滤
}

// NewMessage 创建消息，自动分配 ID 和时间戳
// 参数：
//   - body: 消息内容
//   - headers: 自定义键值对（可以为 nil）
//
// 提示：atomic.AddInt64(&msgID, 1) 生成唯一 ID
func NewMessage(body string, headers map[string]string) *Message {
	return &Message{
		ID:        atomic.AddInt64(&msgID, 1),
		Body:      body,
		Timestamp: time.Now(),
		Headers:   headers,
	}
}

// GetHeader 获取自定义头的值
// 参数：key 键名
// 返回：值和是否存在
func (m *Message) GetHeader(key string) (string, bool) {
	if m.Headers == nil {
		return "", false
	}
	value, exists := m.Headers[key]
	if !exists {
		return "",false
	}
	return value, true	
}

// SetHeader 设置自定义头
// 参数：key 键名，value 值
// 注意：如果 Headers 为 nil，需要先初始化
func (m *Message) SetHeader(key, value string) {
	if m.Headers == nil {
		m.Headers = make(map[string]string)
	}
	m.Headers[key] = value
}
