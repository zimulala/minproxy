package minproxy

import (
	"bytes"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zimulala/minproxy/util"
)

const (
	ConnTimeout      = 5
	ConnRetrys       = 2
	ConnSize         = 600
	ConnReadDeadline = 5
	GetConnErr       = 0
	WriteToConnErr   = 1
	ConnOk           = 2
	ConnOkStr        = ""
)

var (
	ErrBadConfig    = errors.New("bad config err")
	ErrBadBucketKey = errors.New("bad bucket key err")
	ErrGetConn      = errors.New("get conn err")
	ErrWriteToConn  = errors.New("write to conn err")
)

type Sigal struct{}

func (s *Server) CheckConfig(cfg *util.Config) error {
	s.id = cfg.GetInt("id")
	s.ip = cfg.GetString("ip")
	s.port = cfg.GetString("port")
	s.bucketBase = cfg.GetInt("bucket_base")
	buckets := cfg.GetArray("buckets")
	bucketAddrMap := cfg.GetInterface("bucket_addr").(map[string]interface{})

	for _, b := range buckets {
		s.buckets = append(s.buckets, int(b.(float64)))
	}
	for b, addr := range bucketAddrMap {
		bInt, err := strconv.Atoi(b)
		if err != nil {
			return err
		}
		s.bucketAddrMap[bInt] = addr.(string)
	}

	if s.id == -1 || s.ip == "" || s.port == "" {
		return ErrBadConfig
	}

	return nil
}

func InitConnPool(addrMap map[int]string, connP *util.ConnPool) (err error) {
	for _, addr := range addrMap {
		if _, err = connP.NewUnitPool(ConnSize, addr, ConnTimeout, ConnRetrys); err != nil {
			break
		}
	}

	return
}

func GenerateId() int64 {
	return time.Now().UnixNano()
}

func (s *Server) GetAddrs(pkg *Task) (addrs []string, err error) {
	weights := make([]int64, len(pkg.OutInfos))
	addrs = make([]string, len(pkg.OutInfos))

	for i, info := range pkg.OutInfos {
		for _, k := range info.key {
			weights[i] += int64(k)
		}
	}

	s.bucketMux.RLock()
	defer s.bucketMux.RUnlock()
	for i, w := range weights {
		bucket := int(w % int64(len(s.buckets)/s.bucketBase))
		addr, ok := s.bucketAddrMap[bucket]
		if !ok {
			return nil, ErrBadBucketKey
		}
		addrs[i] = addr
	}

	return
}

func (s *Server) GetConns(addrs []string, task *Task) (err error) {
	if len(task.OutInfos) == 1 {
		if task.OutInfos[0].conn, err = s.connPool.GetConn(addrs[0]); err == nil {
			err = task.OutInfos[0].conn.Write(task.OutInfos[0].data)
		}
		if err != nil {
			task.OutInfos[0].connAddr = addrs[0]
		}
		return
	}

	isErr := uint32(ConnOk)
	wg := sync.WaitGroup{}

	for i, info := range task.OutInfos {
		wg.Add(1)
		go func() {
			if info.conn, err = s.connPool.GetConn(addrs[i]); err != nil {
				info.connAddr = addrs[i]
				atomic.StoreUint32(&isErr, GetConnErr)
				wg.Done()
				return
			}
			if err = info.conn.Write(info.data); err != nil {
				info.connAddr = addrs[i]
				atomic.StoreUint32(&isErr, WriteToConnErr)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if isErr == GetConnErr {
		err = ErrGetConn
	} else if isErr == WriteToConnErr {
		err = ErrWriteToConn
	}

	return
}

func (s *Server) ReleaseConns(pkg *Task) {
	for _, info := range pkg.OutInfos {
		if info.connAddr == ConnOkStr {
			s.connPool.PutConn(info.conn.Addr(), info.conn)
			continue
		}
		s.connPool.PutConn(info.connAddr, nil)
	}
}

func GetVal(s []byte) (val []byte, err error) {
	size := len(s)
	idx := bytes.IndexByte(s, '\n')
	if idx < 0 || size < idx+1 || idx+1 > size-2 {
		log.Println("GetVal, s:", string(s), " idx:", idx, " size:", size)
		return nil, ErrBadReqFormat
	}

	val = s[idx+1 : size-2]

	return
}
