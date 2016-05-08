package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis"
)

var (
	relayers        = make(map[int]lib.Relayer)
	relayersCreated = 0
	relayersConfig  *lib.Config
	configFileName  string
	done            = make(chan bool)
	sigs            = make(chan os.Signal, 1)
)

func getNewServer(conf lib.RelayerConfig) (srv lib.Relayer, err error) {
	switch conf.Protocol {
	case "redis":
		srv, err = redis.New(conf, done)
	}
	return
}

func startOrReload() bool {
	// Check config is OK
	conf, err := lib.ReadConfig(configFileName)
	if err != nil {
		log.Println("Bad configuration", err)
		return false
	}
	relayersConfig = conf

	for _, conf := range relayersConfig.Relayer {
		srv, ok := relayers[conf.Listen]
		if !ok {
			// Start a new relayer
			newServer, err := getNewServer(conf)
			if err == nil {
				lib.Debugf("Starting new relayer from %d to %s", newServer.Port(), conf.Url)
				relayersCreated++
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
	return true
}

func main() {

	flag.StringVar(&configFileName, "c", "relayer.conf", "Configuration filename")
	flag.Parse()

	if startOrReload() {

		// Listen for reload signals
		signal.Notify(sigs, syscall.SIGHUP, syscall.SIGUSR1)
		go func() {
			for {
				_ = <-sigs
				startOrReload()
			}
		}()

		for i := 0; i < relayersCreated; i++ {
			<-done
		}
		os.Exit(0)
	}
	os.Exit(1)
}
