package main

import (
	"flag"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
)

func main() {
	done := make(chan bool)
	relayers := make(map[int]lib.Relayer)

	var config lib.Config
	var configFileName string

	flag.StringVar(&configFileName, "c", "relayer.conf", "Configuration filename")
	flag.Parse()

	lib.ReadConfig(configFileName, &config)

	for _, r := range config.Relayer {
		switch r.Protocol {
		case "redis":
			srv, err := redis.New(r, done)
			if err == nil {
				if e := srv.Serve(); e == nil {
					relayers[srv.Port()] = srv
				}
			}
		}
	}

	for i := 0; i < len(relayers); i++ {
		<-done
	}
}
