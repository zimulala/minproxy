package mincluster

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	LineNoStr   = "*"
	DataLenStr  = "$"
	ArgSplitStr = "\r\n"
)

var (
	ErrBadCmdFormat = errors.New("bad cmd format err")
)

var (
	OpError uint8 = 0xFF
)

type Task struct {
	Opcode  uint8
	Id      int64
	OutConn *net.TCPConn
	Buf     []byte
}

func UnmarshalPkg(pkg *Task) (key []byte, err error) {
	elements := strings.SplitN(string(pkg.Buf), ArgSplitStr, -1)
	if !strings.Contains(elements[0], LineNoStr) {
		return key, ErrBadCmdFormat
	}
	lines, err := strconv.Atoi(elements[0][len(LineNoStr):])
	if err != nil || lines != 3 {
		return key, ErrBadCmdFormat
	}

	l, err := strconv.Atoi(strings.Trim(elements[3], DataLenStr))
	if err != nil || l != len(elements[4]) {
		return key, ErrBadCmdFormat
	}
	key = []byte(elements[4])

	return
}

func IsErrTask(task *Task) (err bool) {
	if task.Opcode == OpError {
		err = true
	}

	return
}

//TODO:
func PackErrorReply(res *Task, msg string) {
	res.Opcode = OpError

	return
}

func ReadFromConn(c *net.TCPConn) (pkg *Task, err error) {
	c.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	reader := bufio.NewReader(net.Conn(c))
	dataStr, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	if !strings.HasPrefix(dataStr, LineNoStr) {
		return nil, ErrBadCmdFormat
	}
	linesStr := strings.Trim(strings.Trim(dataStr, LineNoStr), ArgSplitStr)
	lines, err := strconv.Atoi(linesStr)
	if err != nil {
		return
	}
	data := []byte(dataStr)
	for i := 0; i < lines*2; i++ {
		buf, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		data = append(data, buf...)
	}

	return &Task{Id: GenerateId(), OutConn: c, Buf: data}, nil
}

//TODO:
func WriteToConn(c *net.TCPConn, pkg *Task) (err error) {
	_, err = io.ReadFull(c, pkg.Buf)

	return
}
