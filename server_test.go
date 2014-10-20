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
	cfgPath = "cfg.json"
	addr    = "127.0.0.1:9000"
	//addr = "127.0.0.1:6379"
)

var s = NewServer()

var writeTests = []struct {
	args []interface{}
	data string
}{
	{
		[]interface{}{"DEL", "key"},
		"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n",
	},
	{
		[]interface{}{"GETSET", "key", "val"},
		"*3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$3\r\nval\r\n",
	},
	{
		[]interface{}{"GETSET", "key", ""},
		"*3\r\n$6\r\nGETSET\r\n$3\r\nkey\r\n$0\r\n\r\n",
	},
	{
		[]interface{}{"GET", "key"},
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
	},
	{
		[]interface{}{"ZADD", "salary", 9000, "tom"},
		"*4\r\n$4\r\nZADD\r\n$6\r\nsalary\r\n$4\r\n9000\r\n$3\r\ntom\r\n",
	},
	{
		[]interface{}{"ZADD", "salary", 15000, "lily"},
		"*4\r\n$4\r\nZADD\r\n$6\r\nsalary\r\n$5\r\n15000\r\n$4\r\nlily\r\n",
	},
	{
		[]interface{}{"ZADD", "salary", 33000, "lala"},
		"*4\r\n$4\r\nZADD\r\n$6\r\nsalary\r\n$5\r\n33000\r\n$4\r\nlala\r\n",
	},
	{
		[]interface{}{"ZCARD", "salary"},
		"*2\r\n$5\r\nZCARD\r\n$6\r\nsalary\r\n",
	},
	{
		[]interface{}{"HGET", "myhash", "foo"},
		"*3\r\n$4\r\nHGET\r\n$6\r\nmyhash\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"HDEL", "myhash", "foo"},
		"*3\r\n$4\r\nHDEL\r\n$6\r\nmyhash\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"HSET", "myhash", "foo", 2},
		"*4\r\n$4\r\nHSET\r\n$6\r\nmyhash\r\n$3\r\nfoo\r\n$1\r\n2\r\n",
	},
	{
		[]interface{}{"HIncrBy", "myhash", "foo", 2},
		"*4\r\n$7\r\nHIncrBy\r\n$6\r\nmyhash\r\n$3\r\nfoo\r\n$1\r\n2\r\n",
	},
	{
		[]interface{}{"HGET", "myhash", "foo"},
		"*3\r\n$4\r\nHGET\r\n$6\r\nmyhash\r\n$3\r\nfoo\r\n",
	},
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
		[]interface{}{"TYPE", "foo"},
		"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"EXISTS", "foo"},
		"*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"PERSIST", "foo"},
		"*2\r\n$7\r\nPERSIST\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"EXPIRE", "foo", 10},
		"*3\r\n$6\r\nEXPIRE\r\n$3\r\nfoo\r\n$2\r\n10\r\n",
	},
	{
		[]interface{}{"PERSIST", "foo"},
		"*2\r\n$7\r\nPERSIST\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"TTL", "foo"},
		"*2\r\n$3\r\nTTL\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"EXPIREAT", "foo", 1293840000},
		"*3\r\n$8\r\nEXPIREAT\r\n$3\r\nfoo\r\n$10\r\n1293840000\r\n",
	},
	{
		[]interface{}{"EXPIREAT", "foo", 1293840000},
		"*3\r\n$8\r\nEXPIREAT\r\n$3\r\nfoo\r\n$10\r\n1293840000\r\n",
	},
	{
		[]interface{}{"DEL", "foo"},
		"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n",
	},
	{
		[]interface{}{"DEL", "foo"},
		"*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n",
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
	conn, err := redis.DialTimeout("tcp", addr, 10*time.Second, 0, 0)
	if err != nil {
		t.Fatalf("dial err:%+v", err)
	}

	for _, tt := range writeTests {
		reply, err := conn.Do(tt.args[0].(string), tt.args[1:]...)
		if err != nil {
			t.Errorf("Do(%v) returned error %v", tt.args, err)
			continue
		}
		t.Log(tt.args[0].(string), " reply:", reply)
	}
}
