package mq

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// Server Broker 服务端
type Server struct {
	addr     string
	broker   *Broker
	ctx      context.Context    // 全局 context，Shutdown 时 cancel
	cancel   context.CancelFunc // 取消函数
	listener net.Listener       // 保存 listener 引用，Shutdown 时关闭
}

// NewServer 创建服务端实例
func NewServer(addr string) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		addr:   addr,
		broker: NewBroker(),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start 启动监听（阻塞）
func (s *Server) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	log.Printf("Broker listening on %s", s.addr)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Shutdown 关闭 listener 后，Accept 会报错
			// 检查 context 是否已取消，是则退出循环
			if s.ctx.Err() != nil {
				break
			}
			log.Printf("accept error: %v", err)
			continue
		}
		go s.handleConnection(conn)
	}
	log.Printf("Broker stopped accepting connections")
	return nil
}

// Shutdown 优雅关闭服务器
// 1. 取消全局 context → 通知所有订阅 goroutine 退出
// 2. 关闭 listener → 导致 Accept 报错，退出循环
// 3. 等待一段时间让已有连接处理完成
func (s *Server) Shutdown(timeout time.Duration) {
	log.Println("server shutting down...")
	s.cancel()           // 通知所有 goroutine 退出
	s.listener.Close()   // 停止接受新连接
	time.Sleep(timeout)  // 等待已有连接处理完成
	log.Println("server stopped")
}

// handleConnection 处理单个连接
// 注意：不要在这里调用 s.cancel()，那是全局的
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("client connected: %s", conn.RemoteAddr())

	// 每个连接用自己的 context（从全局 ctx 派生）
	// Shutdown 取消全局 ctx 时，所有连接都会收到通知
	ctx := s.ctx

	for {
		cmd, body, err := Decode(conn)
		if err != nil {
			if err == io.EOF {
				log.Printf("client disconnected: %s", conn.RemoteAddr())
			} else {
				log.Printf("connection error from %s: %v", conn.RemoteAddr(), err)
			}
			break
		}
		fields := ParseBody(cmd, body)
		if err := s.handleCommand(ctx, conn, cmd, fields); err != nil {
			conn.Write(Encode(CmdError, err.Error()))
		}
	}
}

// handleCommand 根据命令类型分发处理
func (s *Server) handleCommand(ctx context.Context, conn net.Conn, cmd uint32, fields []string) error {
	switch cmd {
	case CmdDeclareExchange:
		if len(fields) != 2 {
			return fmt.Errorf("invalid declare exchange: need 2 fields")
		}
		s.broker.DeclareExchange(fields[0], parseExchangeType(fields[1]))
		conn.Write(Encode(CmdOK, ""))
	case CmdDeclareQueue:
		if len(fields) != 1 {
			return fmt.Errorf("invalid declare queue: need 1 field")
		}
		s.broker.DeclareQueue(fields[0])
		conn.Write(Encode(CmdOK, ""))
	case CmdBind:
		if len(fields) != 3 {
			return fmt.Errorf("invalid bind: need 3 fields")
		}
		if err := s.broker.Bind(fields[0], fields[1], fields[2]); err != nil {
			return err
		}
		conn.Write(Encode(CmdOK, ""))
	case CmdPublish:
		if len(fields) < 3 {
			return fmt.Errorf("invalid publish: need >= 3 fields")
		}
		body := strings.Join(fields[2:], "\n")
		msg := NewMessage(body, nil)
		if err := s.broker.Publish(fields[0], fields[1], msg); err != nil {
			return err
		}
		conn.Write(Encode(CmdOK, ""))
	case CmdSubscribe:
		if len(fields) != 1 {
			return fmt.Errorf("invalid subscribe: need 1 field")
		}
		queue, err := s.broker.Consume(fields[0])
		if err != nil {
			return err
		}
		conn.Write(Encode(CmdOK, ""))
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				msg := queue.Pop(5 * time.Second)
				if msg == nil {
					continue
				}
				if _, err := conn.Write(Encode(CmdDeliver, fmt.Sprintf("%d\n%s", msg.ID, msg.Body))); err != nil {
					return
				}
			}
		}()
	default:
		return fmt.Errorf("unknown command: %d", cmd)
	}
	return nil
}

// parseExchangeType 解析交换器类型字符串
func parseExchangeType(s string) ExchangeType {
	switch s {
	case "fanout":
		return ExchangeTypeFanout
	case "topic":
		return ExchangeTypeTopic
	default:
		return ExchangeTypeDirect
	}
}

// parseInt64 字符串转 int64
func parseInt64(s string) int64 {
	id, _ := strconv.ParseInt(s, 10, 64)
	return id
}
