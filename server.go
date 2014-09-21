package mincluster

import (
	"net"

	"mincluster/util"
)

type Server struct {
	id            int
	ip            string
	port          string
	connPool      *util.ConnPool
	buckets       []int
	bucketAddrMap map[int]string //key: bucket, val: serverAddr
}

func NewServer() *Server {
	return &Server{
		connPool:      util.NewConnPool(),
		bucketAddrMap: make(map[int]string)}
}

func (s *Server) Start(cfg *util.Config) error {
	if err := s.CheckConfig(cfg); err != nil {
		return err
	}

	if err := InitConnPool(s.bucketAddrMap, s.connPool); err != nil {
		return err
	}

	return s.ListenAndServe()
}

func (s *Server) ListenAndServe() (err error) {
	l, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		return
	}

	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go s.Serve(c)
	}

	l.Close()

	return
}

func (s *Server) handleReply(c *net.TCPConn, taskCh chan *Task, exitCh chan Sigal) {
	for {
		select {
		case task := <-taskCh:
			if IsErrTask(task) {
				WriteToConn(c, task)
				break
			}

			res, err := ReadFromConn(task.OutConn)
			if err != nil {
				PackErrorReply(res, err.Error())
				s.connPool.PutConn(task.OutConn.RemoteAddr().String(), nil)
			} else {
				s.connPool.PutConn(task.OutConn.RemoteAddr().String(), task.OutConn)
			}
			WriteToConn(c, res)
		case <-exitCh:
			return
		}
	}

	return
}

func (s *Server) handleRequest(req *Task) (err error) {
	var addr string
	key, err := UnmarshalPkg(req)
	if err != nil {
		return
	}

	addr, err = s.GetAddr(key)
	if req.OutConn, err = s.connPool.GetConn(addr); err == nil {
		err = WriteToConn(req.OutConn, req)
	}
	if err != nil {
		PackErrorReply(req, err.Error())
		err = nil
	}

	return
}

func (s *Server) Serve(c net.Conn) {
	conn, _ := c.(*net.TCPConn)
	conn.SetKeepAlive(true)
	conn.SetNoDelay(true)
	taskCh := make(chan *Task, 1024)
	exitCh := make(chan Sigal, 1)

	go s.handleReply(conn, taskCh, exitCh)

	for {
		req, err := ReadFromConn(conn)
		if err != nil {
			close(exitCh)
			break
		}
		if err = s.handleRequest(req); err != nil {
			close(exitCh)
			break
		}
		taskCh <- req
	}

	conn.Close()
}
