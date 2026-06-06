package mq

// ============================================================
// Queue —— 并发安全的消息队列
//
// 核心难点：
//   - 并发安全：多个 goroutine 同时 Push/Pop
//   - 阻塞等待：Pop 时如果队列为空，带超时等待
//   - 通知机制：Push 后唤醒等待的消费者
// ============================================================

import (
	"sync"
	"time"
)

// Queue 并发安全的消息队列
type Queue struct {
	Name     string
	messages []*Message      // 消息切片
	mu       sync.Mutex      // 保护 messages
	notify   chan struct{}    // 通知通道：有新消息时通知消费者
}

// NewQueue 创建队列
// notify 缓冲为1：Push 发信号后不会阻塞
func NewQueue(name string) *Queue {
	return &Queue{
		Name: name,
		messages: make([]*Message,0),
		notify: make(chan struct{}, 1),
	}
}

// Push 消息入队（并发安全）
// 步骤：
//   1. 加锁，append 消息，解锁
//   2. 非阻塞地往 notify 发信号
func (q *Queue) Push(msg *Message) {
	q.mu.Lock()
	q.messages = append(q.messages, msg)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Pop 消息出队，带超时阻塞等待
// 参数：timeout 最大等待时间
// 返回：消息指针，超时返回 nil
//
// 步骤：
//   1. 加锁检查队列是否有消息，有则取出返回
//   2. 没消息：解锁，用 select 等待 notify 或超时
//   3. 收到通知：回到步骤1重新检查
//
// 为什么用 timeout 而不是无限等待？
//   - 连接断开时，超时可以让 goroutine 自然退出，避免泄漏
func (q *Queue) Pop(timeout time.Duration) *Message {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		// 先检查队列是否有消息
		q.mu.Lock()
		if len(q.messages) > 0 {
			msg := q.messages[0]
			q.messages = q.messages[1:]
			q.mu.Unlock()
			return msg
		}
		q.mu.Unlock()

		// 没消息，等待通知或超时
		select {
		case <-q.notify:
			// 收到通知，循环回去重新检查
		case <-timer.C:
			return nil // 超时
		}
	}
}

// Len 返回队列当前长度（并发安全）
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}
