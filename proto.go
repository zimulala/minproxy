package mincluster

import (
	"errors"
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
	//req.Id Increment

	return
}

//TODO:
func WriteToConn(c *net.TCPConn, pkg *Task) (err error) {
	return
}
