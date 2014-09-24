package mincluster

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"strconv"
	"time"
	//"io"
)

const (
	LineNoStr   = "*"
	DataLenStr  = "$"
	ArgSplitStr = "\r\n"
)

var (
	LineNoBytes   = []byte("*")
	DataLenBytes  = []byte("$")
	ArgSplitBytes = []byte("\r\n")
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
	elements := bytes.SplitN(pkg.Buf, ArgSplitBytes, -1)
	if !bytes.Contains(elements[0], LineNoBytes) {
		return key, ErrBadCmdFormat
	}

	l, err := strconv.Atoi(string(bytes.Trim(elements[3], DataLenStr)))
	if err != nil || l != len(elements[4]) {
		return key, ErrBadCmdFormat
	}
	key = elements[4]

	return
}

func IsErrTask(task *Task) (err bool) {
	if task.Opcode == OpError {
		err = true
	}

	return
}

func PackErrorReply(res *Task, msg string) {
	res.Opcode = OpError
	res.Buf = []byte("-" + msg)

	return
}

func ReadReqs(c *net.TCPConn) (pkg *Task, err error) {
	c.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	reader := bufio.NewReader(net.Conn(c))
	data, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}

	if !bytes.HasPrefix(data, LineNoBytes) {
		return nil, ErrBadCmdFormat
	}
	linesBuf := bytes.Trim(bytes.Trim(data, LineNoStr), ArgSplitStr)
	lines, err := strconv.Atoi(string(linesBuf))
	if err != nil {
		return
	}
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
func Write(c *net.TCPConn, pkg *Task) (err error) {
	writer := bufio.NewWriter(net.Conn(c))
	_, err = writer.Write((pkg.Buf))
	if err != nil {
		return
	}
	err = writer.Flush()
	//_, err = io.ReadFull(c, pkg.Buf)

	return
}

//TODO:
func ReadReply(c *net.TCPConn, pkg *Task) (err error) {
	c.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	reader := bufio.NewReader(net.Conn(c))
	if pkg.Buf, err = reader.ReadBytes('\n'); err != nil {
		return
	}
	if !bytes.Contains(pkg.Buf, DataLenBytes) {
		return
	}

	data, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}
	pkg.Buf = append(pkg.Buf, data...)
	print("ReadReply end, buf:", string(pkg.Buf), "\n")

	return
}
