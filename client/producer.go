package client

// ============================================================
// Producer —— 生产者客户端
//
// 封装 TCP 连接和协议细节
// 用户只需调用 Declare、Bind、Publish
// ============================================================

import (
	"fmt"
	"minimq/mq"
	"net"
)

// Producer 生产者客户端
type Producer struct {
	conn net.Conn
}

// NewProducer 创建生产者，连接到 broker
func NewProducer(addr string) (*Producer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("connect failed: %v", err)
	}
	return &Producer{conn: conn}, nil
}

func (p *Producer) DeclareExchange(name, exchangeType string) error {
	return p.sendAndReceive(mq.CmdDeclareExchange, name+"\n"+exchangeType)
}

func (p *Producer) DeclareQueue(name string) error {
	return p.sendAndReceive(mq.CmdDeclareQueue, name)
}

func (p *Producer) Bind(exchange, queue, routingKey string) error {
	return p.sendAndReceive(mq.CmdBind, exchange+"\n"+queue+"\n"+routingKey)
}

func (p *Producer) Publish(exchange, routingKey, body string) error {
	return p.sendAndReceive(mq.CmdPublish, exchange+"\n"+routingKey+"\n"+body)
}

func (p *Producer) Close() error {
	return p.conn.Close()
}

// sendAndReceive 发送请求并检查响应
func (p *Producer) sendAndReceive(cmd uint32, body string) error {
	_, err := p.conn.Write(mq.Encode(cmd, body))
	if err != nil {
		return fmt.Errorf("send failed: %v", err)
	}
	respCmd, respBody, err := mq.Decode(p.conn)
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
