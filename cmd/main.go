package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/zimulala/minproxy"
	"github.com/zimulala/minproxy/util"
)

var (
	cfgPath = flag.String("cfg", "/tmp/cfg.json", "configure path")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := util.LoadConfigFile(*cfgPath)
	pprof := cfg.GetString("prof_port")
	if pprof == "" {
		log.Println("bad config")
		return
	}

	go func() {
		log.Fatalln("failed to listen and serve, err:", http.ListenAndServe(":"+pprof, nil))
	}()

	s := minproxy.NewServer()
	if err := s.Start(cfg); err != nil {
		log.Println("failed to start server, err:", err)
	}
}
