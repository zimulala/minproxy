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
	TagBignBytes  = []byte{'{'}
	TagEndBytes   = []byte{'}'}
	LineNumBytes  = []byte{'*'}
	DataLenBytes  = []byte{'$'}
	ArgSplitBytes = []byte("\r\n")
)

var (
	ErrBadCmdFormat = errors.New("bad cmd format err")
	ErrBadArgsNum   = errors.New("bad args num err")
	ErrReadConn     = errors.New("read conn err")
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

func (t *Task) getMKeys(e [][]byte) {
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
		t.OutInfos = append(t.OutInfos, info)
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
func (t *Task) UnmarshalPkg() (err error) {
	elements := bytes.SplitN(t.Buf, ArgSplitBytes, -1)
	if !bytes.HasPrefix(elements[0], LineNumBytes) {
		t.OutInfos = append(t.OutInfos, &UnitPkg{uId: 0, key: elements[0], data: t.Buf})
		return
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
			t.getMKeys(elements)
			break
		}

		t.OutInfos = append(t.OutInfos, &UnitPkg{uId: 0, key: elements[4], data: t.Buf})
		if bytes.Contains(t.OutInfos[0].key, TagBignBytes) || bytes.Contains(t.OutInfos[0].key, TagEndBytes) {
			index := bytes.Index(t.OutInfos[0].key, TagBignBytes)
			t.OutInfos[0].key = t.OutInfos[0].key[index+1 : len(t.OutInfos[0].key)-1]
		}
		// argLen, err := strconv.Atoi(string(bytes.Trim(elements[3], DataLenStr)))
		// if err != nil || argLen != len(key) {
		// 	return ErrBadCmdFormat
		// }
	}

	return
}

func (p *UnitPkg) ReadReply() (err error) {
	p.conn.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	reader := bufio.NewReader(net.Conn(p.conn))
	if p.data, err = reader.ReadBytes('\n'); err != nil {
		return
	}
	if !bytes.HasPrefix(p.data, LineNumBytes) && !bytes.HasPrefix(p.data, DataLenBytes) {
		return
	}

	if bytes.HasPrefix(p.data, DataLenBytes) {
		if bufL, err := strconv.Atoi(string(Trims(p.data, DataLenStr, ArgSplitStr))); err != nil || bufL < 0 {
			return err
		}

		buf, err := reader.ReadBytes('\n')
		if err == nil {
			p.data = append(p.data, buf...)
		}

		return err
	}

	return readMutilLinesData(reader, &p.data)
}

func (t *Task) MergeReplys() (err error) {
	lines := len(t.OutInfos)
	if lines == 1 {
		t.Buf = t.OutInfos[0].data
		return
	}

	t.Buf = append(LineNumBytes, byte(lines))
	for _, info := range t.OutInfos {
		if info.badConn {
			return ErrReadConn
		}
		t.Buf = append(t.Buf, info.data...)
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

func ReadReqs(c *net.TCPConn, reader *bufio.Reader) (t *Task, err error) {
	data, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}

	if bytes.HasPrefix(data, LineNumBytes) {
		if err = readMutilLinesData(reader, &data); err != nil {
			return
		}
	}

	return &Task{Id: GenerateId(), Buf: data}, nil
}

func Write(c *net.TCPConn, buf []byte) (err error) {
	_, err = c.Write(buf)

	return
}
