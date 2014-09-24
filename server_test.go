package mincluster

import (
	"github.com/garyburd/redigo/redis"
	"math"
	"runtime"
	"testing"
	"time"

	"mincluster/util"
)

const (
	cfgPath = "/home/lala/workspace/src/mincluster/cfg.json"
	addr    = "127.0.0.1:54320"
)

var s = NewServer()

var writeTests = []struct {
	args []interface{}
	data string
}{
	{
		[]interface{}{"SET", "foo", "bar"},
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
	},
	{
		[]interface{}{"SET", "foo", "bar"},
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
	},
	{
		[]interface{}{"GET", "foo"},
		"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"SET", "foo", byte(100)},
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n",
	},
	{
		[]interface{}{"SET", "foo", 100},
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n",
	},
	{
		[]interface{}{"SET", "foo", int64(math.MinInt64)},
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$20\r\n-9223372036854775808\r\n",
	},
	{
		[]interface{}{"SET", "foo", float64(1349673917.939762)},
		"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$21\r\n1.349673917939762e+09\r\n",
	},
	{
		[]interface{}{"SET", "", []byte("foo")},
		"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"SET", nil, []byte("foo")},
		"*3\r\n$3\r\nSET\r\n$0\r\n\r\n$3\r\nfoo\r\n",
	},
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	cfg := util.LoadConfigFile(cfgPath)
	go s.Start(cfg)

	time.Sleep(5 * time.Second)
}

func TestBasic(t *testing.T) {
	conn, err := redis.DialTimeout("tcp", addr, 3*time.Second, 10*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("dial err:%+v", err)
	}

	for _, tt := range writeTests {
		reply, err := conn.Do(tt.args[0].(string), tt.args[1:]...)
		if err != nil {
			t.Errorf("Do(%v) returned error %v", tt.args, err)
			continue
		}
		t.Log("reply:", reply)
	}
}
