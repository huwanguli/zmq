package mq

// ============================================================
// Protocol —— 二进制帧协议
//
// 帧格式：
//   [命令类型 uint32][体长度 uint32][消息体]
//        4字节          4字节        N字节
//
// 为什么用二进制头？
//   - 精确读取 N 字节，解决 TCP 粘包/半包问题
//   - 消息体可以包含任意内容（\n、JSON、二进制）
// ============================================================

import (
	"encoding/binary"
	"io"
	"strings"
)

// 命令类型常量
const (
	CmdDeclareExchange uint32 = 1
	CmdDeclareQueue    uint32 = 2
	CmdBind            uint32 = 3
	CmdUnbind          uint32 = 4
	CmdPublish         uint32 = 5
	CmdSubscribe       uint32 = 6
	CmdDeliver         uint32 = 7
	CmdAck             uint32 = 8
	CmdNack            uint32 = 9
	CmdOK              uint32 = 10
	CmdError           uint32 = 11
)

// Encode 把命令编码成二进制帧
// 参数：cmd 命令类型，body 消息体
// 返回：[]byte 编码后的帧
func Encode(cmd uint32, body string) []byte {
	buf := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(buf[:4], cmd)
	binary.BigEndian.PutUint32(buf[4:8], uint32(len(body)))
	copy(buf[8:], body)
	return buf
}

// Decode 从连接读取一帧
// 参数：r 读取器（通常是 net.Conn）
// 返回：cmd 命令类型，body 消息体，err 错误
//
// 关键：用 io.ReadFull 精确读取，不能用 r.Read（可能读不满）
func Decode(r io.Reader) (uint32, string, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, "", err
	}
	cmd := binary.BigEndian.Uint32(header[:4])
	length := binary.BigEndian.Uint32(header[4:8])
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, "", err
	}
	return cmd, string(body), nil
}

// ParseBody 根据命令类型解析体内容
func ParseBody(cmd uint32, body string) []string {
	switch cmd {
	case CmdDeclareExchange:
		return splitByFirstN(body, 1) // "name\ntype"
	case CmdDeclareQueue:
		return splitByFirstN(body, 0) // "name"
	case CmdBind, CmdUnbind:
		return splitByFirstN(body, 2) // "exchange\nqueue\nroutingKey"
	case CmdPublish:
		return splitByFirstN(body, 2) // "exchange\nroutingKey\nbody"
	case CmdSubscribe:
		return splitByFirstN(body, 0) // "queue"
	case CmdDeliver:
		return splitByFirstN(body, 1) // "id\nbody"
	case CmdAck, CmdNack:
		return splitByFirstN(body, 0) // "id"
	case CmdError:
		return splitByFirstN(body, 0) // "reason"
	default:
		return nil
	}
}

// splitByFirstN 按 \n 分割，只切前 n 个，剩余全部保留
func splitByFirstN(s string, n int) []string {
	if n <= 0 || !strings.Contains(s, "\n") {
		return []string{s}
	}
	return strings.SplitN(s, "\n", n+1)
}
