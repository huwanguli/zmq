package mq

// ============================================================
// Exchange —— 消息路由核心
//
// 不同类型用不同规则路由消息
// 接口统一，具体实现各自独立
//
// Direct: routing key 精确匹配
// Fanout: 广播到所有绑定的队列
// Topic:  通配符匹配（* 匹配一个词，# 匹配多个词）
// ============================================================

import "strings"

// Exchange 交换器接口
type Exchange interface {
	// Route 根据路由键查找匹配的队列
	Route(routingKey string, msg *Message) []string
	// Bind 绑定队列到交换器
	Bind(routingKey, queueName string)
}

// Binding 绑定规则：Exchange → Queue 的路由关系
type Binding struct {
	RoutingKey string // 路由键（Direct/Topic 用）
	QueueName  string // 目标队列名
}

// ============================================================
// DirectExchange —— 精确匹配
// routing key 完全一致才投递
// ============================================================

type DirectExchange struct {
	Name     string
	Bindings []Binding
}

func NewDirectExchange(name string) *DirectExchange {
	return &DirectExchange{
		Name:     name,
		Bindings: make([]Binding, 0),
	}
}

func (e *DirectExchange) Route(routingKey string, msg *Message) []string {
	queues := make([]string, 0)
	for _, b := range e.Bindings {
		if b.RoutingKey == routingKey {
			queues = append(queues, b.QueueName)
		}
	}
	return queues
}

func (e *DirectExchange) Bind(routingKey, queueName string) {
	// 去重：避免重复绑定同一条规则
	for _, b := range e.Bindings {
		if b.RoutingKey == routingKey && b.QueueName == queueName {
			return
		}
	}
	e.Bindings = append(e.Bindings, Binding{RoutingKey: routingKey, QueueName: queueName})
}

func (e *FanoutExchange) Bind(routingKey, queueName string) {
	for _, b := range e.Bindings {
		if b.RoutingKey == routingKey && b.QueueName == queueName {
			return
		}
	}
	e.Bindings = append(e.Bindings, Binding{RoutingKey: routingKey, QueueName: queueName})
}

func (e *TopicExchange) Bind(routingKey, queueName string) {
	for _, b := range e.Bindings {
		if b.RoutingKey == routingKey && b.QueueName == queueName {
			return
		}
	}
	e.Bindings = append(e.Bindings, Binding{RoutingKey: routingKey, QueueName: queueName})
}

// ============================================================
// FanoutExchange —— 广播
// 忽略 routing key，投递到所有绑定的队列
// ============================================================

type FanoutExchange struct {
	Name     string
	Bindings []Binding
}

func NewFanoutExchange(name string) *FanoutExchange {
	return &FanoutExchange{
		Name:     name,
		Bindings: make([]Binding, 0),
	}
}

func (e *FanoutExchange) Route(routingKey string, msg *Message) []string {
	queues := make([]string, 0)
	for _, b := range e.Bindings {
		queues = append(queues, b.QueueName)
	}
	return queues
}

// ============================================================
// TopicExchange —— 通配符匹配
//
// 规则：
//   * 匹配恰好一个词
//   # 匹配零个或多个词
//
// 示例：
//   routingKey = "order.created"
//   绑定 "order.*"     → 匹配 ✓
//   绑定 "order.#"     → 匹配 ✓
//   绑定 "#.created"   → 匹配 ✓
//   绑定 "user.*"      → 不匹配 ✗
// ============================================================

type TopicExchange struct {
	Name     string
	Bindings []Binding
}

func NewTopicExchange(name string) *TopicExchange {
	return &TopicExchange{
		Name:     name,
		Bindings: make([]Binding, 0),
	}
}

func (e *TopicExchange) Route(routingKey string, msg *Message) []string {
	queues := make([]string, 0)
	for _, b := range e.Bindings {
		if topicMatch(b.RoutingKey, routingKey) {
			queues = append(queues, b.QueueName)
		}
	}
	return queues
}

// topicMatch 通配符匹配
// 参数：pattern 绑定模式（如 "order.*"），routingKey 实际路由键（如 "order.created"）
// 返回：是否匹配
func topicMatch(pattern, routingKey string) bool {
	patternParts := strings.Split(pattern, ".")
	routingParts := strings.Split(routingKey, ".")

	m, n := len(routingParts), len(patternParts)

	dp := make([][]bool, m+1)
	for i := range dp {
		dp[i] = make([]bool, n+1)
	}

	dp[0][0] = true

	for j := 1; j <= n; j++ {
		if patternParts[j-1] == "#" {
			dp[0][j] = dp[0][j-1]
		}
	}

	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if patternParts[j-1] == routingParts[i-1] || patternParts[j-1] == "*" {
				dp[i][j] = dp[i-1][j-1]
			} else if patternParts[j-1] == "#" {
				dp[i][j] = dp[i][j-1] || dp[i-1][j]
			}
		}
	}

	return dp[m][n]
}
