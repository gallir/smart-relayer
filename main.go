package main

import (
	"flag"

	"github.com/gallir/smart-relayer/redis"
	"github.com/gallir/smart-relayer/tools"
)

type Relayer interface {
	//	New(*tools.RelayerConfig) *Relayer
	Serve() error
}

func main() {
	/*
		server, err := redis.NewServer(redis.DefaultConfig())
		if err != nil {
			panic(err)
		}
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	*/
	done := make(chan bool)
	relayers := 0

	var config tools.Config
	var configFileName string

	flag.StringVar(&configFileName, "c", "relayer.conf", "Configuration filename")
	flag.Parse()

	tools.ReadConfig(configFileName, &config)

	for _, r := range config.Relayer {
		switch r.Protocol {
		case "redis":
			srv, err := redis.New(r, done)
			if err == nil {
				if e := srv.Serve(); e == nil {
					relayers++
				}
			}
		}
	}

	for i := 0; i < relayers; i++ {
		<-done
	}
}
