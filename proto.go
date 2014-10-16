package mincluster

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"strconv"
	"time"
)

const (
	LineNumStr  = "*"
	DataLenStr  = "$"
	ArgSplitStr = "\r\n"
)

var (
	LineNumBytes  = []byte("*")
	DataLenBytes  = []byte("$")
	ArgSplitBytes = []byte("\r\n")
)

var (
	ErrBadCmdFormat = errors.New("bad cmd format err")
	ErrBadArgsNum   = errors.New("bad args num err")
)

var (
	OpError uint8 = 0xFF
)

type Task struct {
	Opcode    uint8
	Id        int64
	OutReader *bufio.Reader
	OutConn   *net.TCPConn
	Buf       []byte
}

/*
*3\r\n
$4\r\n
HSET\r\n
$6\r\n
myhash\r\n
$5\r\n
field1\r\n
$3\r\n
foo\r\n
*/
func UnmarshalPkg(pkg *Task) (key []byte, err error) {
	print("req:", string(pkg.Buf))
	elements := bytes.SplitN(pkg.Buf, ArgSplitBytes, -1)
	if !bytes.Contains(elements[0], LineNumBytes) {
		return key, ErrBadCmdFormat
	}

	var lenBuf []byte
	switch elements[2][0] {
	case 'z', 'Z':
		if len(elements) < 9 {
			return nil, ErrBadArgsNum
		}
		lenBuf = elements[7]
		key = elements[8]
	default:
		if len(elements) < 5 {
			return nil, ErrBadArgsNum
		}
		lenBuf = elements[3]
		key = elements[4]
	}

	l, err := strconv.Atoi(string(bytes.Trim(lenBuf, DataLenStr)))
	if err != nil || l != len(key) {
		return nil, ErrBadCmdFormat
	}

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
	res.Buf = []byte("-" + msg + "\r\n")

	return
}

func readMutilLinesData(r *bufio.Reader, data *[]byte) (err error) {
	lines, err := strconv.Atoi(string(Trims(*data, LineNumStr, ArgSplitStr)))
	if err != nil {
		return
	}

	for i := 0; i < lines*2; i++ {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		*data = append(*data, buf...)

		if !bytes.HasPrefix(buf, DataLenBytes) {
			continue
		}
		if bufL, err := strconv.Atoi(string(Trims(buf, DataLenStr, ArgSplitStr))); err != nil || bufL <= 0 {
			return err
		}
	}

	return
}

func ReadReqs(c *net.TCPConn, reader *bufio.Reader) (pkg *Task, err error) {
	//c.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	data, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}

	if !bytes.HasPrefix(data, LineNumBytes) {
		return nil, ErrBadCmdFormat
	}
	if err = readMutilLinesData(reader, &data); err != nil {
		return
	}

	return &Task{Id: GenerateId(), Buf: data}, nil
}

func Write(c *net.TCPConn, buf []byte) (err error) {
	_, err = c.Write(buf)

	return
}

func ReadReply(pkg *Task) (err error) {
	pkg.OutConn.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	reader := bufio.NewReader(net.Conn(pkg.OutConn))
	if pkg.Buf, err = reader.ReadBytes('\n'); err != nil {
		return
	}
	if !bytes.HasPrefix(pkg.Buf, LineNumBytes) && !bytes.HasPrefix(pkg.Buf, DataLenBytes) {
		return
	}

	if bytes.HasPrefix(pkg.Buf, DataLenBytes) {
		if bufL, err := strconv.Atoi(string(Trims(pkg.Buf, DataLenStr, ArgSplitStr))); err != nil || bufL <= 0 {
			return err
		}

		buf, err := reader.ReadBytes('\n')
		if err == nil {
			pkg.Buf = append(pkg.Buf, buf...)
		}
		return err
	}

	return readMutilLinesData(reader, &pkg.Buf)
}
