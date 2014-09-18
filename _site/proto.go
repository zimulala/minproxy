package mincluster

import (
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
)

const (
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

//TODO:
func UnmarshalPkg(pkg *Task) (key []byte, err error) {
	elements := strings.SplitN(string(pkg.Buf), ArgSplitStr, -1)
	if !strings.Contains(elements[0], "*") {
		return key, ErrBadCmdFormat
	}
	lines, err := strconv.Atoi(elements[0][len("*"):])
	if err != nil || lines != 3 {
		return key, ErrBadCmdFormat
	}

	l, err := strconv.Atoi(elements[3])
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

//TODO:
func ReadFromConn(c *net.TCPConn) (pkg *Task, err error) {
	reader := bufio.NewReader(net.Conn(c))
	dataStr, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	if !strings.HasPrefix(dataStr, "*") {
		return nil, ErrBadCmdFormat
	}
	linesStr := strings.Trim(strings.Trim(dataStr, "*"), "\r")
	lines, err := strconv.Atoi(linesStr)
	if err != nil {
		return
	}
	data := []byte(dataStr)
	for i := 0; i < lines; i++ {
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
