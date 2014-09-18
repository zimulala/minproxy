package util

import (
	"runtime"
	"sync"
	"testing"
)

const (
	Addr  = "127.0.0.1:80"
	PutOp = "put"
	GetOp = "get"
)

func TestBasicConnPool(t *testing.T) {
	p := NewConnPool()
	p.NewUnitPool(-1, Addr, 3, -1)

	c, err := p.GetConn(Addr)
	if err != nil {
		t.Log("get conn err:", err)
		t.FailNow()
	}
	if err = p.PutConn(Addr, c); err != nil {
		t.Log("put conn err:", err)
		t.FailNow()
	}

	//Addr don't exist
	if _, err := p.GetConn(Addr + "not exist"); err != ErrNotExistUnitPool {
		t.Log("get conn err:", err)
		t.Fail()
	}
	if err = p.PutConn(Addr+"not exist", c); err != ErrNotExistUnitPool {
		t.Log("put conn err:", err)
		t.Fail()
	}
}

func MutilThreadsOperation(loops int, b *testing.B, logTab string, f func(Addr string) error) {
	wg := sync.WaitGroup{}

	for i := 0; i < loops; i++ {
		wg.Add(1)
		go func() {
			if err := f(Addr); err != nil {
				b.Log(logTab+" err:", err)
				b.FailNow()
			}

			wg.Done()
		}()
	}

	wg.Wait()
}

func BenchmarkConnPool(b *testing.B) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	p := NewConnPool()
	size := 55
	timeout := 3
	retrys := 2
	p.NewUnitPool(size, Addr, timeout, retrys)

	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		MutilThreadsOperation(b.N, b, GetOp, func(Addr string) error {
			c, err := p.GetConn(Addr)
			if err == nil {
				c.Close()
			}
			return err
		})
		wg.Done()
	}()
	go func() {
		wg.Add(1)
		MutilThreadsOperation(b.N, b, PutOp, func(Addr string) error { err := p.PutConn(Addr, nil); return err })
		wg.Done()
	}()

	wg.Wait()
}
