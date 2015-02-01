package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"

	"github.com/mincluster"
	"github.com/mincluster/util"
)

var (
	cfgPath = flag.String("cfg", "/tmp/cfg.json", "configure path")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())

	cfg := util.LoadConfigFile(*cfgPath)
	pprof := cfg.GetString("Pprof")
	if pprof == "" {
		log.Println("bad config")
		return
	}

	go func() {
		log.Fatalln("failed to listen and serve, err:", http.ListenAndServe(":"+pprof, nil))
	}()

	s := mincluster.NewServer()
	if err := s.Start(cfg); err != nil {
		log.Println("failed to start server, err:", err)
	}
}
