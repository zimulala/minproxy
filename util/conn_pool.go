package util

import (
	"errors"
	"sync"
	"time"
)

const (
	DefaultSize          = 50
	DefaultTrys          = 1
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
	trys    int
	addr    string
	pool    chan *Conn
}

func NewConnPool() *ConnPool {
	return &ConnPool{unitPools: make(map[string]*UnitConnPool)}
}

func (connp *ConnPool) NewUnitPool(size int, addr string, timeout, trys int) (p *UnitConnPool, err error) {
	if size < 0 {
		size = DefaultSize
	}
	if trys <= 0 {
		trys = DefaultTrys
	}
	if addr == "" {
		return nil, ErrAddrEmpty
	}

	p = &UnitConnPool{addr: addr, size: size, timeout: timeout, trys: trys, pool: make(chan *Conn, size)}
	for i := 0; i < size; i++ {
		p.pool <- nil
	}
	if err = p.Ping(); err != nil {
		return
	}
	connp.SetUintPool(addr, p)

	return
}

func (p *UnitConnPool) Ping() (err error) {
	c, err := p.Get()
	if err != nil {
		return
	}
	defer p.Put(c)

	return
}

func (p *UnitConnPool) Get() (c *Conn, err error) {
	select {
	case c = <-p.pool:
		if c != nil {
			return
		}
	default:
	}

	for i := 0; i < p.trys; i++ {
		if c, err = NewCon(ConnType, p.addr, time.Duration(p.timeout)*time.Second); err == nil {
			break
		}
	}
	if err != nil {
		return
	}

	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	return
}

func (p *UnitConnPool) Put(conn *Conn) (err error) {
	select {
	case p.pool <- conn:
	default:
		if conn.c != nil {
			conn.Close()
		}
	}

	return
}

func (connp *ConnPool) GetConn(addr string) (c *Conn, err error) {
	p, ok := connp.GetUintPool(addr)
	if !ok {
		return nil, ErrNotExistUnitPool
	}

	return p.Get()
}

func (connp *ConnPool) PutConn(addr string, conn *Conn) (err error) {
	p, ok := connp.GetUintPool(addr)
	if !ok {
		if conn.c != nil {
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
