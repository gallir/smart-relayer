package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
)

var (
	relayers       = make(map[int]lib.Relayer)
	config         lib.Config
	configFileName string
	done           = make(chan bool)
	sigs           = make(chan os.Signal, 1)
)

func getNewServer(conf lib.RelayerConfig) (srv lib.Relayer, err error) {
	switch conf.Protocol {
	case "redis":
		srv, err = redis.New(conf, done)
	}
	return
}

func startOrReload() {
	lib.ReadConfig(configFileName, &config)

	for _, conf := range config.Relayer {
		srv, ok := relayers[conf.Listen]
		if !ok {
			// Start a new relayer
			newServer, err := getNewServer(conf)
			if err == nil {
				lib.Debugf("Starting new relayer from %d to %s", newServer.Port(), conf.Url)
				if e := newServer.Start(); e == nil {
					relayers[newServer.Port()] = newServer
				}
			}
		} else {
			// The relayer exists, reload it
			lib.Debugf("Reloading relayer from %d to %s", conf.Listen, conf.Url)
			srv.Reload(&conf)
		}
	}
}

func main() {

	flag.StringVar(&configFileName, "c", "relayer.conf", "Configuration filename")
	flag.Parse()

	startOrReload()

	// Listen for reload signals
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGUSR1)
	go func() {
		for {
			_ = <-sigs
			startOrReload()
		}
	}()

	for i := 0; i < len(relayers); i++ {
		<-done
	}
}
