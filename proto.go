package mincluster

import (
	"net"
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
func UnmarshalPkg(pkg *Task) (key int, err error) {
	return
}

//TODO:
func IsErrTask(task *Task) (err bool) {
	return
}

//TODO:
func PackErrorReply(res *Task, msg string) {
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
