package mq

import (
	"testing"
	"time"
)

// ============================================================
// Message 测试 —— 覆盖正常情况 + 边界情况 + 并发
// ============================================================

func TestNewMessage(t *testing.T) {
	msg := NewMessage("hello", nil)
	if msg == nil {
		t.Fatal("NewMessage 返回了 nil")
	}
	if msg.ID <= 0 {
		t.Errorf("消息 ID 应该大于 0, 实际 %d", msg.ID)
	}
	if msg.Body != "hello" {
		t.Errorf("Body 应该是 hello, 实际 %s", msg.Body)
	}
	if msg.Timestamp.IsZero() {
		t.Error("Timestamp 不应该是零值")
	}
}

func TestMessageIDAutoIncrement(t *testing.T) {
	msg1 := NewMessage("a", nil)
	msg2 := NewMessage("b", nil)
	if msg2.ID != msg1.ID+1 {
		t.Errorf("ID 应该递增: 第一条 %d, 第二条 %d", msg1.ID, msg2.ID)
	}
}

func TestMessageTimestamp(t *testing.T) {
	before := time.Now()
	msg := NewMessage("test", nil)
	after := time.Now()

	if msg.Timestamp.Before(before) || msg.Timestamp.After(after) {
		t.Errorf("Timestamp 应该在创建时间范围内")
	}
}

func TestMessageHeadersNil(t *testing.T) {
	msg := NewMessage("test", nil)
	if msg.Headers != nil {
		t.Error("传入 nil headers 时，Headers 应该为 nil")
	}
}

func TestGetHeaderNilMap(t *testing.T) {
	msg := NewMessage("test", nil)
	_, ok := msg.GetHeader("key")
	if ok {
		t.Error("nil Headers 应该返回 false")
	}
}

func TestGetHeader(t *testing.T) {
	headers := map[string]string{"x-retry": "3"}
	msg := NewMessage("test", headers)

	val, ok := msg.GetHeader("x-retry")
	if !ok || val != "3" {
		t.Errorf("期望 x-retry=3, 实际 %s, exists=%v", val, ok)
	}
}

func TestGetHeaderMissing(t *testing.T) {
	headers := map[string]string{"x-retry": "3"}
	msg := NewMessage("test", headers)

	_, ok := msg.GetHeader("x-missing")
	if ok {
		t.Error("不存在的 key 应该返回 false")
	}
}

func TestSetHeader(t *testing.T) {
	msg := NewMessage("test", nil)
	msg.SetHeader("x-retry", "3")

	val, ok := msg.GetHeader("x-retry")
	if !ok || val != "3" {
		t.Errorf("SetHeader 后 GetHeader 期望 x-retry=3, 实际 %s, exists=%v", val, ok)
	}
}

func TestSetHeaderOverwrite(t *testing.T) {
	headers := map[string]string{"x-retry": "3"}
	msg := NewMessage("test", headers)
	msg.SetHeader("x-retry", "5")

	val, _ := msg.GetHeader("x-retry")
	if val != "5" {
		t.Errorf("SetHeader 覆盖后期望 x-retry=5, 实际 %s", val)
	}
}

// 并发测试：多个 goroutine 同时创建消息
func TestMessageConcurrent(t *testing.T) {
	done := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		go func() {
			msg := NewMessage("concurrent", nil)
			if msg == nil {
				t.Error("并发创建消息失败")
			}
			done <- true
		}()
	}
	for i := 0; i < 100; i++ {
		<-done
	}
}
