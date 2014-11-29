package mincluster

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"

	"mincluster/util"
)

type Server struct {
	id       int
	ip       string
	port     string
	connPool *util.ConnPool

	reBuckets     int
	buckets       []int
	bucketAddrMap map[int]string //key: bucket, val: serverAddr
	bucketMux     sync.RWMutex
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

func ReadReply(task *Task) {
	wg := sync.WaitGroup{}
	for _, info := range task.OutInfos {
		wg.Add(1)
		go func() {
			if err := info.ReadReply(); err != nil {
				info.connAddr = info.conn.Addr()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (s *Server) handleReply(c *net.TCPConn, taskCh chan *Task, exitCh chan Sigal) {
	for {
		select {
		case task := <-taskCh:
			if task.IsErrTask() {
				Write(c, *task.Resp)
				s.ReleaseConns(task)
				break
			}

			if len(task.OutInfos) == 1 {
				if err := task.OutInfos[0].ReadReply(); err != nil {
					task.OutInfos[0].connAddr = task.OutInfos[0].conn.Addr()
				}
			} else {
				ReadReply(task)
			}

			if err := task.MergeReplys(); err != nil {
				task.PackErrorReply(err.Error())
			}
			Write(c, *task.Resp)
		case <-exitCh:
			return
		}
	}

	return
}

func (s *Server) GetConnsToWrite(addrs []string, task *Task) (err error) {
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
			defer wg.Done()
			if info.conn, err = s.connPool.GetConn(addrs[i]); err != nil {
				info.connAddr = addrs[i]
				atomic.StoreUint32(&isErr, GetConnErr)
				return
			}
			if err = info.conn.Write(info.data); err != nil {
				info.connAddr = addrs[i]
				atomic.StoreUint32(&isErr, WriteToConnErr)
			}
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

func (s *Server) handleRequest(req *Task) (err error) {
	if err = req.UnmarshalPkg(); err != nil {
		return
	}

	addrs, err := s.GetAddrs(req)
	if err != nil {
		req.PackErrorReply(err.Error())
		return nil
	}

	if err = s.GetConnsToWrite(addrs, req); err != nil {
		req.PackErrorReply(err.Error())
	}

	return nil
}

func (s *Server) Serve(c net.Conn) {
	conn, _ := c.(*net.TCPConn)
	conn.SetKeepAlive(true)
	conn.SetNoDelay(true)
	reader := bufio.NewReader(c)
	taskCh := make(chan *Task, 1024)
	exitCh := make(chan Sigal, 1)

	go s.handleReply(conn, taskCh, exitCh)

	for {
		req, err := ReadReqs(conn, reader)
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
