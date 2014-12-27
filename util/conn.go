package util

import (
	"bufio"
	"net"
	"time"
)

const DefaultMinReadBufferSize = 1024

type Conn struct {
	addr string
	c    *net.TCPConn
	R    *bufio.Reader
}

func NewCon(network, addr string, timeout time.Duration) (*Conn, error) {
	c, err := net.DialTimeout(network, addr, timeout)
	if err != nil {
		return nil, err
	}

	return &Conn{addr: addr, c: c.(*net.TCPConn), R: bufio.NewReaderSize(c, DefaultMinReadBufferSize)}, nil
}

func (c *Conn) Write(buf []byte) (err error) {
	_, err = c.c.Write(buf)

	return
}

func (c *Conn) ReadBytes(p byte) ([]byte, error) {
	return c.R.ReadBytes(p)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.c.SetReadDeadline(t)
}

func (c *Conn) SetKeepAlive(b bool) error {
	return c.c.SetKeepAlive(b)
}

func (c *Conn) SetNoDelay(b bool) error {
	return c.c.SetNoDelay(b)
}

func (c Conn) Addr() string {
	return c.addr
}

func (c *Conn) Close() {
	c.c.Close()
}
