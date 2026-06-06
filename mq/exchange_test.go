package mq

import "testing"

// ============================================================
// Exchange 测试 —— 覆盖三种类型 + 边界情况
// ============================================================

// ---- Direct Exchange ----

func TestDirectRoute(t *testing.T) {
	ex := NewDirectExchange("order")
	ex.Bindings = append(ex.Bindings, Binding{"created", "q1"})
	ex.Bindings = append(ex.Bindings, Binding{"created", "q2"})
	ex.Bindings = append(ex.Bindings, Binding{"deleted", "q3"})

	queues := ex.Route("created", nil)
	if len(queues) != 2 {
		t.Fatalf("期望匹配2个队列, 实际 %d", len(queues))
	}
	if queues[0] != "q1" || queues[1] != "q2" {
		t.Errorf("期望 [q1, q2], 实际 %v", queues)
	}
}

func TestDirectNoMatch(t *testing.T) {
	ex := NewDirectExchange("order")
	ex.Bindings = append(ex.Bindings, Binding{"created", "q1"})

	queues := ex.Route("unknown", nil)
	if len(queues) != 0 {
		t.Errorf("期望无匹配, 实际 %v", queues)
	}
}

// ---- Fanout Exchange ----

func TestFanoutRoute(t *testing.T) {
	ex := NewFanoutExchange("broadcast")
	ex.Bindings = append(ex.Bindings, Binding{"ignored", "q1"})
	ex.Bindings = append(ex.Bindings, Binding{"ignored", "q2"})

	queues := ex.Route("any-key", nil)
	if len(queues) != 2 {
		t.Fatalf("期望广播到2个队列, 实际 %d", len(queues))
	}
}

func TestFanoutIgnoresRoutingKey(t *testing.T) {
	ex := NewFanoutExchange("broadcast")
	ex.Bindings = append(ex.Bindings, Binding{"", "q1"})

	queues1 := ex.Route("key-a", nil)
	queues2 := ex.Route("key-b", nil)
	if len(queues1) != 1 || len(queues2) != 1 {
		t.Error("Fanout 应该忽略 routing key")
	}
}

// ---- Topic Exchange ----

func TestTopicStar(t *testing.T) {
	ex := NewTopicExchange("topic")
	ex.Bindings = append(ex.Bindings, Binding{"order.*", "q1"})

	if len(ex.Route("order.created", nil)) != 1 {
		t.Error("order.* 应该匹配 order.created")
	}
	if len(ex.Route("order.cancelled", nil)) != 1 {
		t.Error("order.* 应该匹配 order.cancelled")
	}
	if len(ex.Route("order.created.v2", nil)) != 0 {
		t.Error("order.* 不应该匹配 order.created.v2")
	}
}

func TestTopicHash(t *testing.T) {
	ex := NewTopicExchange("topic")
	ex.Bindings = append(ex.Bindings, Binding{"order.#", "q1"})

	if len(ex.Route("order.created", nil)) != 1 {
		t.Error("order.# 应该匹配 order.created")
	}
	if len(ex.Route("order.created.v2", nil)) != 1 {
		t.Error("order.# 应该匹配 order.created.v2")
	}
	if len(ex.Route("order", nil)) != 1 {
		t.Error("order.# 应该匹配 order")
	}
}

func TestTopicMiddleHash(t *testing.T) {
	ex := NewTopicExchange("topic")
	ex.Bindings = append(ex.Bindings, Binding{"#.created", "q1"})

	if len(ex.Route("order.created", nil)) != 1 {
		t.Error("#.created 应该匹配 order.created")
	}
	if len(ex.Route("user.created", nil)) != 1 {
		t.Error("#.created 应该匹配 user.created")
	}
	if len(ex.Route("a.b.created", nil)) != 1 {
		t.Error("#.created 应该匹配 a.b.created")
	}
}

func TestTopicNoMatch(t *testing.T) {
	ex := NewTopicExchange("topic")
	ex.Bindings = append(ex.Bindings, Binding{"order.*", "q1"})

	if len(ex.Route("user.created", nil)) != 0 {
		t.Error("order.* 不应该匹配 user.created")
	}
}

// ---- topicMatch 单元测试 ----

func TestTopicMatch(t *testing.T) {
	tests := []struct {
		pattern string
		key     string
		want    bool
	}{
		{"order.*", "order.created", true},
		{"order.*", "order.created.v2", false},
		{"order.#", "order.created", true},
		{"order.#", "order.created.v2", true},
		{"order.#", "order", true},
		{"#.created", "order.created", true},
		{"#.created", "a.b.created", true},
		{"*.*", "a.b", true},
		{"*.*", "a.b.c", false},
		{"#", "anything", true},
		{"#", "a.b.c", true},
	}
	for _, tt := range tests {
		got := topicMatch(tt.pattern, tt.key)
		if got != tt.want {
			t.Errorf("topicMatch(%q, %q) = %v, 期望 %v", tt.pattern, tt.key, got, tt.want)
		}
	}
}

// ---- 接口验证 ----

var _ Exchange = (*DirectExchange)(nil)
var _ Exchange = (*FanoutExchange)(nil)
var _ Exchange = (*TopicExchange)(nil)
