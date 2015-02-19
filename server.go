package minproxy

import (
	"bufio"
	"net"
	"sync"

	"github.com/zimulala/minproxy/util"
)

type Server struct {
	id       int
	ip       string
	port     string
	connPool *util.ConnPool

	bucketBase    int
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

func (s *Server) Serve(c net.Conn) {
	conn, _ := c.(*net.TCPConn)
	conn.SetKeepAlive(true)
	conn.SetNoDelay(true)
	reader := bufio.NewReader(c)
	taskCh := make(chan *Task, 1024)
	exitCh := make(chan Sigal, 1)

	go s.handleReplys(conn, taskCh, exitCh)

	for {
		req, err := ReadReqs(conn, reader)
		if err != nil {
			close(exitCh)
			break
		}
		if err = s.handleReqs(req); err != nil {
			close(exitCh)
			break
		}
		taskCh <- req
	}
	conn.Close()
}

func ReadReqs(c *net.TCPConn, reader *bufio.Reader) (t *Task, err error) {
	t = &Task{Id: GenerateId()}
	if t.Raw, err = ReadReqData(reader); err != nil {
		return
	}
	if len(t.Raw) <= 0 {
		err = ErrBadReqFormat
	}

	return
}

func (s *Server) handleReqs(req *Task) (err error) {
	if err = req.UnmarshalPkg(); err != nil {
		return
	}

	addrs, err := s.GetAddrs(req)
	if err != nil {
		req.PackErrorReply(err.Error())
		return nil
	}

	if err = s.GetConns(addrs, req); err != nil {
		req.PackErrorReply(err.Error())
	}

	return nil
}

func (s *Server) handleReplys(c *net.TCPConn, taskCh chan *Task, exitCh chan Sigal) {
	for {
		select {
		case task := <-taskCh:
			if task.IsErrTask() {
				Write(c, *task.Resp)
				s.ReleaseConns(task)
				break
			}

			ReadReplys(task)
			if err := task.MergeReplys(); err != nil {
				task.PackErrorReply(err.Error())
			}
			Write(c, *task.Resp)
			s.ReleaseConns(task)
		case <-exitCh:
			return
		}
	}

	return
}

func ReadReplys(task *Task) {
	if len(task.OutInfos) == 1 {
		if err := task.OutInfos[0].ReadReply(); err != nil {
			task.OutInfos[0].connAddr = task.OutInfos[0].conn.Addr()
		}
		return
	}

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
