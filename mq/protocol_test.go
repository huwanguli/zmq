package mq

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// ============================================================
// Protocol 测试 —— 覆盖编码解码 + 各命令类型 + 边界情况
// ============================================================

// ---- 编码解码往返测试 ----

func TestEncodeDecodeRoundTrip(t *testing.T) {
	body := "ex\nkey\nhello world with spaces"
	frame := Encode(CmdPublish, body)

	// 用 Decode 解回来
	r := bytes.NewReader(frame)
	gotCmd, gotBody, err := Decode(r)
	if err != nil {
		t.Fatal(err)
	}
	if gotCmd != CmdPublish {
		t.Errorf("命令类型期望 %d, 实际 %d", CmdPublish, gotCmd)
	}
	if gotBody != body {
		t.Errorf("体内容期望 '%s', 实际 '%s'", body, gotBody)
	}
}

func TestEncodeEmptyBody(t *testing.T) {
	frame := Encode(CmdOK, "")
	if len(frame) != 8 {
		t.Errorf("空体帧长度期望 8, 实际 %d", len(frame))
	}
	cmd := binary.BigEndian.Uint32(frame[0:4])
	if cmd != CmdOK {
		t.Errorf("命令类型期望 %d, 实际 %d", CmdOK, cmd)
	}
	bodyLen := binary.BigEndian.Uint32(frame[4:8])
	if bodyLen != 0 {
		t.Errorf("体长度期望 0, 实际 %d", bodyLen)
	}
}

func TestEncodeBodyWithNewline(t *testing.T) {
	// 消息体包含换行，不应被截断
	body := "ex\nkey\nline1\nline2\nline3"
	frame := Encode(CmdPublish, body)

	r := bytes.NewReader(frame)
	_, gotBody, _ := Decode(r)
	if gotBody != body {
		t.Errorf("消息体被截断: '%s'", gotBody)
	}
}

func TestDecodeIncompleteHeader(t *testing.T) {
	// 只有 4 字节，应该报错
	r := bytes.NewReader([]byte{0, 0, 0, 1})
	_, _, err := Decode(r)
	if err == nil {
		t.Error("不完整的 header 应该返回错误")
	}
}

func TestDecodeIncompleteBody(t *testing.T) {
	// header 声明 10 字节体，但实际只有 3 字节
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], CmdPublish)
	binary.BigEndian.PutUint32(header[4:8], 10)
	body := []byte("abc")
	r := bytes.NewReader(append(header, body...))
	_, _, err := Decode(r)
	if err == nil {
		t.Error("不完整的 body 应该返回错误")
	}
}

// ---- ParseBody 测试 ----

func TestParseBodyPublish(t *testing.T) {
	fields := ParseBody(CmdPublish, "ex\nkey\norder #1")
	if len(fields) != 3 || fields[2] != "order #1" {
		t.Errorf("PUBLISH 解析错误: %v", fields)
	}
}

func TestParseBodyPublishWithNewline(t *testing.T) {
	fields := ParseBody(CmdPublish, "ex\nkey\nline1\nline2")
	if len(fields) != 3 || fields[2] != "line1\nline2" {
		t.Errorf("消息体被切分: %v", fields)
	}
}

func TestParseBodySubscribe(t *testing.T) {
	fields := ParseBody(CmdSubscribe, "order_queue")
	if len(fields) != 1 || fields[0] != "order_queue" {
		t.Errorf("SUBSCRIBE 解析错误: %v", fields)
	}
}

func TestParseBodyDeliver(t *testing.T) {
	fields := ParseBody(CmdDeliver, "123\nhello world")
	if len(fields) != 2 || fields[0] != "123" || fields[1] != "hello world" {
		t.Errorf("DELIVER 解析错误: %v", fields)
	}
}

func TestParseBodyBind(t *testing.T) {
	fields := ParseBody(CmdBind, "ex\nq\nkey")
	if len(fields) != 3 || fields[0] != "ex" || fields[1] != "q" || fields[2] != "key" {
		t.Errorf("BIND 解析错误: %v", fields)
	}
}

func TestParseBodyError(t *testing.T) {
	fields := ParseBody(CmdError, "something went wrong")
	if len(fields) != 1 || fields[0] != "something went wrong" {
		t.Errorf("ERROR 解析错误: %v", fields)
	}
}

func TestParseBodyAck(t *testing.T) {
	fields := ParseBody(CmdAck, "42")
	if len(fields) != 1 || fields[0] != "42" {
		t.Errorf("ACK 解析错误: %v", fields)
	}
}
