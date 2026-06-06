package mq

// ============================================================
// Broker —— 消息代理的核心调度中心
//
// 管理 Exchange 和 Queue，实现消息的发布与路由
//
// 消息流转：
//   Publish(exchange, routingKey, body)
//     → Exchange.Route(routingKey) → 匹配的队列列表
//     → 每个队列 Push(msg)
// ============================================================

import (
	"fmt"
	"sync"
)

// ExchangeType 交换器类型
type ExchangeType int

const (
	ExchangeTypeDirect ExchangeType = iota
	ExchangeTypeFanout
	ExchangeTypeTopic
)

// Broker 消息代理
type Broker struct {
	queues    map[string]*Queue   // 队列名 → 队列实例
	exchanges map[string]Exchange // 交换器名 → 交换器实例
	qmu       sync.RWMutex       // 保护 queues
	emu       sync.RWMutex       // 保护 exchanges
}

// NewBroker 创建 Broker 实例
func NewBroker() *Broker {
	return &Broker{
		queues:    make(map[string]*Queue),
		exchanges: make(map[string]Exchange),
	}
}

// DeclareExchange 声明交换器
// 如果已存在则忽略，不存在则根据类型创建
func (b *Broker) DeclareExchange(name string, exchangeType ExchangeType) {
	b.emu.Lock()
	defer b.emu.Unlock()
	if _, exists := b.exchanges[name]; exists {
		return
	}
	switch exchangeType {
	case ExchangeTypeDirect:
		b.exchanges[name] = NewDirectExchange(name)
	case ExchangeTypeFanout:
		b.exchanges[name] = NewFanoutExchange(name)
	case ExchangeTypeTopic:
		b.exchanges[name] = NewTopicExchange(name)
	}
}

// DeclareQueue 声明队列
// 如果已存在则忽略，不存在则创建
func (b *Broker) DeclareQueue(name string) {
	b.qmu.Lock()
	defer b.qmu.Unlock()
	if _, exists := b.queues[name]; !exists {
		b.queues[name] = NewQueue(name)
	}
}

// Bind 绑定队列到交换器
// 参数：exchangeName 交换器名，queueName 队列名，routingKey 路由键
// 返回：error（交换器不存在或队列不存在时返回错误）
func (b *Broker) Bind(exchangeName, queueName, routingKey string) error {
	b.emu.RLock()
	exchange, exists := b.exchanges[exchangeName]
	b.emu.RUnlock()
	if !exists {
		return fmt.Errorf("exchange %s does not exist", exchangeName)
	}
	b.qmu.RLock()
	_, exists = b.queues[queueName]
	b.qmu.RUnlock()
	if !exists {
		return fmt.Errorf("queue %s does not exist", queueName)
	}
	// 通过接口方法绑定，而不是直接访问 Bindings 字段
	exchange.Bind(routingKey, queueName)
	return nil
}

// Publish
// 参数：exchangeName 交换器名，routingKey 路由键，body 消息内容
// 返回：error
func (b *Broker) Publish(exchangeName, routingKey string, msg *Message) error {
	b.emu.RLock()
	exchange, exists := b.exchanges[exchangeName]
	b.emu.RUnlock()
	if !exists {
		return fmt.Errorf("exchange %s does not exist", exchangeName)
	}
	//   2. exchange.Route(routingKey, msg) 得到匹配的队列名
	queues := exchange.Route(routingKey, msg)
	//   3. 加读锁找到每个队列
	b.qmu.RLock()
	for _, queueName := range queues {
		if queue, exists := b.queues[queueName];exists {
			queue.Push(msg)
		}
	}
	b.qmu.RUnlock()
	return nil
}

// Consume 获取队列引用
// 返回：队列实例（消费者从队列里 Pop 消息）
func (b *Broker) Consume(queueName string) (*Queue, error) {
	b.qmu.RLock()
	defer b.qmu.RUnlock()
	queue, exists := b.queues[queueName]
	if !exists {
		return nil, fmt.Errorf("queue %s does not exist", queueName)
	}
	return queue, nil
}
