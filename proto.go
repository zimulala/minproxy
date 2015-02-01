package mincluster

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/zimulala/mincluster/util"
)

const (
	LineNumStr  = "*"
	DataSizeStr = "$"
	ArgSplitStr = "\r\n"
)

var (
	TagBeginByte  = []byte{'{'}
	TagEndBytes   = []byte{'}'}
	TagSplitByte  = []byte{','}
	LineNumBytes  = []byte{'*'}
	DataSizeBytes = []byte{'$'}
	ArgSplitBytes = []byte("\r\n")
)

var (
	ErrBadReqFormat = errors.New("bad req format err")
	ErrBadArgsNum   = errors.New("bad args num err")
	ErrReadConn     = errors.New("read conn err")
)

var (
	OpError uint8 = 0xFF
)

type UnitPkg struct {
	conn     *util.Conn
	uId      int
	key      []byte
	data     []byte
	connAddr string
}

type Task struct {
	Opcode   uint8
	Id       int64
	OutInfos []*UnitPkg
	Raw      [][]byte
	Resp     *[]byte
}

func (t *Task) IsErrTask() (err bool) {
	if t.Opcode == OpError {
		err = true
	}

	return
}

func (t *Task) PackErrorReply(msg string) {
	t.Opcode = OpError
	errMsg := []byte("-" + msg + "\r\n")
	t.Resp = &errMsg

	return
}

func (t *Task) getMKeys(e [][]byte) {
	interval := 2
	val := ArgSplitBytes
	if string(e[2]) == "mset" {
		interval = 4
	}

	begin := 3
	t.OutInfos = make([]*UnitPkg, (len(e)-begin)/interval)
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

func Append(data [][]byte) (buf []byte) {
	for _, b := range data {
		buf = append(buf, b...)
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
	defer func() {
		if err != nil {
			log.Println("UnmarshalPkg, err:", err)
		} else if len(t.OutInfos) > 0 {
			log.Println("UnmarshalPkg, req:", string(t.OutInfos[0].data))
		}
	}()

	if !bytes.HasPrefix(t.Raw[0], LineNumBytes) { //ping
		t.OutInfos = append(t.OutInfos, &UnitPkg{uId: 0, key: t.Raw[0], data: Append(t.Raw)})
		return
	}

	lineN, err := strconv.Atoi(string(t.Raw[0][1 : len(t.Raw[0])-2]))
	if err != nil {
		return
	}
	switch lineN <= 1 {
	case true:
		return ErrBadArgsNum
	case false:
		if len(t.Raw) < 3 {
			return ErrBadArgsNum
		}
		if cmd, err := GetVal(t.Raw[1]); err != nil {
			return err
		} else if string(cmd) == "mset" || string(cmd) == "mget" {
			t.getMKeys(t.Raw)
			break
		}

		key, err := GetVal(t.Raw[2])
		if err != nil {
			return err
		}
		t.OutInfos = append(t.OutInfos, &UnitPkg{uId: 0, key: key, data: Append(t.Raw)})
		if bytes.Contains(t.OutInfos[0].key, TagBeginByte) || bytes.Contains(t.OutInfos[0].key, TagEndBytes) {
			start := bytes.Index(t.OutInfos[0].key, TagBeginByte)
			end := bytes.Index(t.OutInfos[0].key, TagSplitByte)
			if end < 0 {
				end = bytes.Index(t.OutInfos[0].key, TagEndBytes)
			}
			if start < 0 || end < start {
				log.Println("string:", string(t.OutInfos[0].key), " start:", start, " end:", end)
				return ErrBadReqFormat
			}
			t.OutInfos[0].key = t.OutInfos[0].key[start+1 : end]
			log.Println("key:", string(t.OutInfos[0].key))
		}
		// argLen, err := strconv.Atoi(string(bytes.Trim(t.Raw[3], DataSizeStr)))
		// if err != nil || argLen != len(key) {
		// 	return ErrBadReqFormat
		// }
	}

	return
}

func readBulk(r *bufio.Reader, d []byte, data *[]byte) (err error) {
	bufL, err := strconv.Atoi(string(d[1 : len(d)-2]))
	if err != nil {
		return err
	}
	if bufL < 0 {
		return
	}

	buf, err := r.ReadBytes('\n')
	if err == nil {
		*data = append(*data, buf...)
	}

	return
}

func (p *UnitPkg) ReadReply() (err error) {
	p.conn.SetReadDeadline(time.Now().Add(ConnReadDeadline * time.Second))
	if p.data, err = p.conn.ReadBytes('\n'); err != nil {
		return
	}
	if !bytes.HasPrefix(p.data, LineNumBytes) && !bytes.HasPrefix(p.data, DataSizeBytes) {
		return
	}
	if bytes.HasPrefix(p.data, DataSizeBytes) {
		readBulk(p.conn.R, p.data, &p.data)
		return
	}

	return readReplyData(p.conn, &p.data)
}

func readReplyData(r *util.Conn, data *[]byte) (err error) {
	lines, err := strconv.Atoi(string((*data)[1 : len(*data)-2]))
	if err != nil {
		return
	}

	for i := 0; i < lines; i++ {
		buf, err := r.ReadBytes('\n')
		if err != nil {
			return err
		}
		*data = append(*data, buf...)

		if bytes.HasPrefix(buf, DataSizeBytes) {
			readBulk(r.R, buf, data)
			continue
		}

		if bytes.HasPrefix(buf, LineNumBytes) {
			i = -1
			lines, err = strconv.Atoi(string(buf[1 : len(buf)-2]))
		}
	}

	return
}

func (t *Task) MergeReplys() (err error) {
	lines := len(t.OutInfos)
	if lines == 1 {
		if t.OutInfos[0].connAddr != ConnOkStr {
			return ErrReadConn
		}
		t.Resp = &t.OutInfos[0].data
		return
	}

	*t.Resp = append(LineNumBytes, byte(lines))
	for _, info := range t.OutInfos {
		if info.connAddr != ConnOkStr {
			return ErrReadConn
		}
		*t.Resp = append(*t.Resp, info.data...)
	}

	return
}

func readLine(r *bufio.Reader) (b []byte, err error) {
	if b, err = r.ReadBytes('\n'); err != nil {
		return
	}
	l := len(b) - 2
	if l < 0 || b[l] != '\r' {
		err = ErrBadReqFormat
	}

	return
}

//*3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$0\r\n\r\n
func readReqData(r *bufio.Reader) (raws [][]byte, err error) {
	buf, err := readLine(r)
	if err != nil || len(buf) <= 2 {
		return
	}
	lines, err := strconv.Atoi(string(buf[1 : len(buf)-2]))
	if err != nil || lines < 0 {
		return
	}

	switch buf[0] {
	case '+', '-', ':':
		err = ErrBadReqFormat
	case '$':
		raws = make([][]byte, 1)
		s := len(buf)
		raws[0] = make([]byte, s+lines+2)
		copy(raws[0][:s], buf)
		if _, err = io.ReadFull(r, raws[0][s:s+lines]); err != nil {
			return nil, err
		}
		if b, err := readLine(r); err != nil || len(b) != 2 {
			return nil, err
		} else {
			copy(raws[0][(s+lines):(s+lines+2)], b)
		}
	case '*':
		raws = make([][]byte, lines+1)
		raws[0] = buf
		for i := 1; i <= lines; i++ {
			raw, err := readReqData(r)
			if err != nil || len(raw) <= 0 {
				return nil, err
			}
			raws[i] = raw[0]
		}
	case 'P':
		raws = make([][]byte, 1)
		raws[0] = buf
	default:
		err = ErrBadReqFormat
	}

	return
}

func ReadReqs(c *net.TCPConn, reader *bufio.Reader) (t *Task, err error) {
	t = &Task{Id: GenerateId()}
	if t.Raw, err = readReqData(reader); err != nil {
		return
	}
	if len(t.Raw) <= 0 {
		err = ErrBadReqFormat
	}

	return
}

func Write(c *net.TCPConn, buf []byte) (err error) {
	_, err = c.Write(buf)

	return
}
