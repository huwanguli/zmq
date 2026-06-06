package client

// ============================================================
// Consumer —— 消费者客户端
//
// 封装 TCP 连接和协议细节
// 用户只需调用 DeclareQueue 和 Subscribe
// ============================================================

import (
	"fmt"
	"minimq/mq"
	"net"
	"strconv"
)

// MessageHandler 消息处理函数
type MessageHandler func(id int64, body string)

// Consumer 消费者客户端
type Consumer struct {
	conn net.Conn
}

// NewConsumer 创建消费者，连接到 broker
func NewConsumer(addr string) (*Consumer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect failed: %v", err)
	}
	return &Consumer{conn: conn}, nil
}

// DeclareQueue 声明队列
func (c *Consumer) DeclareQueue(name string) error {
	return c.sendAndReceive(mq.CmdDeclareQueue, name)
}

// Subscribe 订阅队列，阻塞接收消息
// handler 每收到一条消息就调用一次
func (c *Consumer) Subscribe(queue string, handler MessageHandler) error {
	// 发送 SUBSCRIBE，读取 OK 确认
	if err := c.sendAndReceive(mq.CmdSubscribe, queue); err != nil {
		return err
	}

	// 循环接收 DELIVER
	for {
		cmd, body, err := mq.Decode(c.conn)
		if err != nil {
			return fmt.Errorf("recv failed: %v", err)
		}
		if cmd == mq.CmdDeliver {
			fields := mq.ParseBody(cmd, body)
			handler(parseID(fields[0]), fields[1])
		} else if cmd == mq.CmdError {
			return fmt.Errorf("server error: %s", body)
		}
		// 忽略其他命令，继续读下一条
	}
}

// Close 关闭连接
func (c *Consumer) Close() error {
	return c.conn.Close()
}

// sendAndReceive 发送请求并检查响应
func (c *Consumer) sendAndReceive(cmd uint32, body string) error {
	_, err := c.conn.Write(mq.Encode(cmd, body))
	if err != nil {
		return fmt.Errorf("send failed: %v", err)
	}
	respCmd, respBody, err := mq.Decode(c.conn)
	if err != nil {
		return fmt.Errorf("recv failed: %v", err)
	}
	if respCmd == mq.CmdError {
		return fmt.Errorf("server error: %s", respBody)
	}
	if respCmd != mq.CmdOK {
		return fmt.Errorf("unexpected response: cmd=%d body=%s", respCmd, respBody)
	}
	return nil
}

// parseID 字符串转 int64
func parseID(s string) int64 {
	id, _ := strconv.ParseInt(s, 10, 64)
	return id
}
