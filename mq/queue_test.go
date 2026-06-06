package mq

import (
	"sync"
	"testing"
	"time"
)

// ============================================================
// Queue 测试 —— 覆盖正常情况 + 边界情况 + 并发 + 超时
// ============================================================

func TestPushPop(t *testing.T) {
	q := NewQueue("test")
	msg := NewMessage("hello", nil)
	q.Push(msg)

	got := q.Pop(time.Second)
	if got == nil {
		t.Fatal("Pop 返回了 nil")
	}
	if got.Body != "hello" {
		t.Errorf("期望 hello, 实际 %s", got.Body)
	}
}

func TestFIFO(t *testing.T) {
	q := NewQueue("test")
	q.Push(NewMessage("a", nil))
	q.Push(NewMessage("b", nil))

	if q.Pop(time.Second).Body != "a" {
		t.Error("应该是 a")
	}
	if q.Pop(time.Second).Body != "b" {
		t.Error("应该是 b")
	}
}

func TestLen(t *testing.T) {
	q := NewQueue("test")
	q.Push(NewMessage("a", nil))
	q.Push(NewMessage("b", nil))
	if q.Len() != 2 {
		t.Errorf("长度应为 2, 实际 %d", q.Len())
	}
	q.Pop(time.Second)
	if q.Len() != 1 {
		t.Errorf("长度应为 1, 实际 %d", q.Len())
	}
}

func TestPopTimeout(t *testing.T) {
	q := NewQueue("test")

	start := time.Now()
	got := q.Pop(100 * time.Millisecond)
	elapsed := time.Since(start)

	if got != nil {
		t.Errorf("空队列 Pop 应该返回 nil, 实际 %v", got)
	}
	if elapsed < 90*time.Millisecond {
		t.Errorf("Pop 应该等待至少 100ms, 实际 %v", elapsed)
	}
}

func TestPopBlockThenPush(t *testing.T) {
	q := NewQueue("test")

	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Push(NewMessage("delayed", nil))
	}()

	got := q.Pop(time.Second)
	if got == nil {
		t.Fatal("应该收到消息, 实际 nil")
	}
	if got.Body != "delayed" {
		t.Errorf("期望 delayed, 实际 %s", got.Body)
	}
}

// 并发测试：多个生产者和消费者同时操作
func TestQueueConcurrent(t *testing.T) {
	q := NewQueue("test")
	var wg sync.WaitGroup

	// 10 个生产者，每个发 100 条消息
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				q.Push(NewMessage("msg", nil))
			}
		}()
	}

	// 等生产者完成
	wg.Wait()

	if q.Len() != 1000 {
		t.Errorf("期望 1000 条消息, 实际 %d", q.Len())
	}
}

// 并发测试：同时 Push 和 Pop
func TestQueueConcurrentPushPop(t *testing.T) {
	q := NewQueue("test")
	var wg sync.WaitGroup
	count := 0
	var mu sync.Mutex

	// 5 个生产者
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				q.Push(NewMessage("msg", nil))
			}
		}()
	}

	// 5 个消费者
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				msg := q.Pop(2 * time.Second)
				if msg != nil {
					mu.Lock()
					count++
					mu.Unlock()
				}
			}
		}()
	}

	wg.Wait()
	if count != 500 {
		t.Errorf("期望消费 500 条消息, 实际 %d", count)
	}
}

// 边界：Pop 超时后再次 Pop 应该能正常工作
func TestPopTimeoutThenPop(t *testing.T) {
	q := NewQueue("test")

	// 第一次 Pop 超时
	got1 := q.Pop(50 * time.Millisecond)
	if got1 != nil {
		t.Error("第一次 Pop 应该超时")
	}

	// Push 一条消息
	q.Push(NewMessage("after-timeout", nil))

	// 第二次 Pop 应该成功
	got2 := q.Pop(time.Second)
	if got2 == nil || got2.Body != "after-timeout" {
		t.Errorf("第二次 Pop 应该收到消息")
	}
}
