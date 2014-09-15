package util

import (
	"errors"
	"net"
	"sync"
	"time"
)

const (
	DefaultSize          = 50
	DefaultRetrys        = 1
	DefaultRetryInterval = 1
	ConnType             = "tcp"
)

var (
	ErrSameAddr         = errors.New("SameAddrError")
	ErrPoolFull         = errors.New("PoolFullError")
	ErrAddrEmpty        = errors.New("AddrEmptyError")
	ErrNotExistUnitPool = errors.New("NotExistUnitPoolErr")
)

type ConnPool struct {
	rwMu      sync.RWMutex
	unitPools map[string]*UnitConnPool
}

type UnitConnPool struct {
	size    int
	timeout int
	retrys  int
	addr    string
	pool    chan *net.TCPConn
}

func NewConnPool() *ConnPool {
	return &ConnPool{unitPools: make(map[string]*UnitConnPool)}
}

func (connp *ConnPool) NewUnitPool(size int, addr string, timeout, retrys int) (p *UnitConnPool, err error) {
	if size < 0 {
		size = DefaultSize
	}
	if retrys <= 0 {
		retrys = DefaultRetrys
	}
	if addr == "" {
		return nil, ErrAddrEmpty
	}

	p = &UnitConnPool{addr: addr, size: size, timeout: timeout, retrys: retrys, pool: make(chan *net.TCPConn, size)}
	for i := 0; i < size; i++ {
		p.pool <- nil
	}
	connp.SetUintPool(addr, p)

	return
}

func (p *UnitConnPool) Get() (c *net.TCPConn, err error) {
	var conn net.Conn
	select {
	case c = <-p.pool:
	default:
	}
	if c != nil {
		return
	}

	for i := 0; i < p.retrys; i++ {
		if conn, err = net.DialTimeout(ConnType, p.addr, time.Duration(p.timeout)*time.Second); err == nil {
			break
		}
	}
	if err != nil {
		return
	}

	c, _ = conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	return
}

func (p *UnitConnPool) Put(conn *net.TCPConn) (err error) {
	select {
	case p.pool <- conn:
	default:
		if conn != nil {
			conn.Close()
		}
	}

	return
}

func (connp *ConnPool) GetConn(addr string) (c *net.TCPConn, err error) {
	p, ok := connp.GetUintPool(addr)
	if !ok {
		return nil, ErrNotExistUnitPool
	}

	return p.Get()
}

func (connp *ConnPool) PutConn(addr string, conn *net.TCPConn) (err error) {
	p, ok := connp.GetUintPool(addr)
	if !ok {
		if conn != nil {
			conn.Close()
		}
		return
	}

	return p.Put(conn)
}

func (connp *ConnPool) SetUintPool(addr string, p *UnitConnPool) {
	connp.rwMu.Lock()
	connp.unitPools[addr] = p
	connp.rwMu.Unlock()
}

func (connp *ConnPool) GetUintPool(addr string) (p *UnitConnPool, ok bool) {
	connp.rwMu.RLock()
	p, ok = connp.unitPools[addr]
	connp.rwMu.RUnlock()

	return
}
