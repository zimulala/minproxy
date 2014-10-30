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

type UnitPkg struct {
	badConn bool
	conn    *net.TCPConn
	uId     int
	key     []byte
	data    []byte
}

type Task struct {
	Opcode   uint8
	Id       int64
	OutInfos []*UnitPkg
	Buf      []byte
}

func (t *Task) IsErrTask() (err bool) {
	if t.Opcode == OpError {
		err = true
	}

	return
}

func (t *Task) PackErrorReply(msg string) {
	t.Opcode = OpError
	t.Buf = []byte("-" + msg + "\r\n")

	return
}

func getMKeys(e [][]byte, pkg *Task) {
	interval := 2
	val := ArgSplitBytes
	if string(e[2]) == "mset" {
		interval = 4
	}

	begin := 3
	for i := begin; i < len(e); i += interval {
		if interval == 4 {
			val = append(bytes.Join([][]byte{e[i+2], e[i+3]}, ArgSplitBytes), ArgSplitBytes...)
		}

		info := &UnitPkg{uId: i - begin, key: e[i+1],
			data: bytes.Join([][]byte{[]byte("*3"), e[1], e[2], e[i], e[i+1], val}, ArgSplitBytes)}
		pkg.OutInfos = append(pkg.OutInfos, info)
	}

	return
}

/*
*4\r\n
$4\r\n
HSET\r\n
$6\r\n
myhash\r\n
$5\r\n
field1\r\n
$0\r\n
\r\n
*/
func UnmarshalPkg(pkg *Task) (err error) {
	elements := bytes.SplitN(pkg.Buf, ArgSplitBytes, -1)
	if !bytes.HasPrefix(elements[0], LineNumBytes) {
		return ErrBadCmdFormat
	}

	lineN, err := strconv.Atoi(string(Trims(elements[0], LineNumStr, ArgSplitStr)))
	if err != nil {
		return
	}
	switch lineN <= 1 {
	case true:
		return ErrBadArgsNum
	case false:
		if len(elements) < 5 {
			return ErrBadArgsNum
		}
		if string(elements[2]) == "mset" || string(elements[2]) == "mget" {
			getMKeys(elements, pkg)
			break
		}

		pkg.OutInfos = append(pkg.OutInfos, &UnitPkg{uId: 0, key: elements[4], data: pkg.Buf})
		// argLen, err := strconv.Atoi(string(bytes.Trim(elements[3], DataLenStr)))
		// if err != nil || argLen != len(key) {
		// 	return ErrBadCmdFormat
		// }
	}

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

		if bytes.HasPrefix(buf, LineNumBytes) {
			i = -1
			lines, err = strconv.Atoi(string(Trims(buf, LineNumStr, ArgSplitStr)))
			continue
		}

		if !bytes.HasPrefix(buf, DataLenBytes) {
			continue
		}
		if bufL, err := strconv.Atoi(string(Trims(buf, DataLenStr, ArgSplitStr))); err != nil || bufL < 0 {
			return err
		}
		// else if bufL == 0 && i == (lines-1)*2 {
		// 	return nil
		// }
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

func ReadReply(pkg *UnitPkg) (err error) {
	pkg.conn.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	reader := bufio.NewReader(net.Conn(pkg.conn))
	if pkg.data, err = reader.ReadBytes('\n'); err != nil {
		return
	}
	if !bytes.HasPrefix(pkg.data, LineNumBytes) && !bytes.HasPrefix(pkg.data, DataLenBytes) {
		return
	}

	if bytes.HasPrefix(pkg.data, DataLenBytes) {
		if bufL, err := strconv.Atoi(string(Trims(pkg.data, DataLenStr, ArgSplitStr))); err != nil || bufL < 0 {
			return err
		}

		buf, err := reader.ReadBytes('\n')
		if err == nil {
			pkg.data = append(pkg.data, buf...)
		}

		return err
	}

	return readMutilLinesData(reader, &pkg.data)
}
