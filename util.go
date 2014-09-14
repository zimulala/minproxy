package mincluster

import (
	"errors"

	"mincluster/util"
)

const (
	ConnTimeout = 5
	ConnRetrys  = 2
	ConnSize    = 100
)

var (
	ErrBadConfig    = errors.New("bad config err")
	ErrBadBucketKey = errors.New("bad bucket key err")
)

type Sigal struct{}

func (s *Server) CheckConfig(cfg *util.Config) error {
	s.id = cfg.GetInt("Id")
	s.ip = cfg.GetString("Ip")
	s.port = cfg.GetString("Port")
	buckets := cfg.GetArray("Buckets")
	bucketAddrMap := cfg.GetInterface("ServerBucket").(map[int]string)

	for _, b := range buckets {
		s.buckets = append(s.buckets, b.(int))
	}
	for b, addr := range bucketAddrMap {
		s.bucketAddrMap[b] = addr
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

func (s *Server) GetAddr(key int) (addr string, err error) {
	var ok bool
	if addr, ok = s.bucketAddrMap[key]; !ok {
		err = ErrBadBucketKey
	}

	return
}
