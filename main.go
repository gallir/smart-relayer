package main

import (
	"errors"
	"fmt"
	"github.com/gallir/smart-relayer/httpTo/httpToFirehose"
	"log"
	"os"
	"os/signal"
	"syscall"

	libdebug "runtime/debug"

	"sync"

	"github.com/gallir/smart-relayer/custom/redis2kvstore"
	"github.com/gallir/smart-relayer/httpTo/httpToAthena"
	"github.com/gallir/smart-relayer/httpproxy"
	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis/cluster"
	"github.com/gallir/smart-relayer/redis/fh"
	"github.com/gallir/smart-relayer/redis/fs"
	"github.com/gallir/smart-relayer/redis/kinesis"
	redis2 "github.com/gallir/smart-relayer/redis/radix"
	"github.com/gallir/smart-relayer/redis/radix.improved/redis"
	"github.com/gallir/smart-relayer/redis/rsqs"
)

const (
	version = "8.8.8"
)

var (
	mutex         sync.Mutex
	relayers      = make(map[string]lib.Relayer)
	totalRelayers = 0
	//relayersConfig *lib.Config
	done = make(chan bool)
	//reloadSig      = make(chan os.Signal, 1)
	//exitSig        = make(chan os.Signal, 1)
)

func getNewServer(conf lib.RelayerConfig) (srv lib.Relayer, err error) {
	switch conf.Protocol {
	case "redis", "redis2":
		srv, err = redis2.New(conf, done)
	case "redis-cluster", "redis-plus":
		srv, err = cluster.New(conf, done)
	case "firehose":
		srv, err = fh.New(conf, done)
	case "httpToFirehose":
		srv, err = httpToFirehose.New(conf, done)
	case "kinesis":
		srv, err = kinesis.New(conf, done)
	case "sqs":
		srv, err = rsqs.New(conf, done)
	case "fs":
		srv, err = fs.New(conf, done)
	case "http":
		srv, err = httpproxy.New(conf, done)
	case "redis2kvstore":
		srv, err = redis2kvstore.New(conf, done)
	case "httpToAthena":
		srv, err = httpToAthena.New(conf, done)
	default:
		err = errors.New("no valid option")
	}
	return
}

func startOrReload() bool {
	mutex.Lock()
	defer mutex.Unlock()

	// Check config is OK
	newConf, err := lib.ReadConfig(lib.GlobalConfig.ConfigFileName)
	if err != nil {
		log.Println("Bad configuration", err)
		return false
	}

	if newConf.GOGC > 100 {
		libdebug.SetGCPercent(newConf.GOGC)
		log.Println("Set GCPercent to", newConf.GOGC)
	} else {
		libdebug.SetGCPercent(100)
	}

	redis.UsePool = newConf.BufferPoolSize
	if redis.UsePool > 0 {
		log.Println("Set UsePool to", redis.UsePool)
	}

	lib.Debugf("%#v", newConf.Relayer)

	newEndpoints := make(map[string]bool)

	for _, conf := range newConf.Relayer {
		endpoint, ok := relayers[conf.Listen]
		newEndpoints[conf.Listen] = true
		if !ok {
			// Start a new relayer
			r, err := getNewServer(conf)
			if err != nil {
				log.Println("E: Error starting relayer", conf.Protocol, conf.URL, err)
				continue
			}
			lib.Debugf("Starting new relayer from %s to %s", conf.Listen, conf.URL)
			totalRelayers++
			if e := r.Start(); e == nil {
				relayers[conf.Listen] = r
			}
		} else {
			// The relayer exists, reload it
			err := endpoint.Reload(&conf)
			if err != nil {
				log.Println("E: Error reloading", conf.Protocol, conf.URL, err)
			}
		}
	}

	for endpoint, r := range relayers {
		_, ok := newEndpoints[endpoint]
		if !ok {
			log.Printf("Deleting old endpoint %s", endpoint)
			delete(relayers, endpoint)
			r.Exit()
		}
	}

	return true
}

func main() {
	setRLimit()
	// Show version and exit
	if lib.GlobalConfig.ShowVersion {
		fmt.Println("smart-relayer version", version)
		showRLimit()
		os.Exit(0)
	}

	if !lib.GlobalConfig.Debug {
		initLogging("smart-relayer")
	}

	go signals()

	if !startOrReload() {
		os.Exit(1)
	}

	for i := 0; i < totalRelayers; i++ {
		<-done
	}
	os.Exit(0)
}

func signals() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGHUP, os.Interrupt)
	for s := range sigs {
		switch s {
		case syscall.SIGHUP:
			startOrReload()
		default:
			for _, r := range relayers {
				r.Exit()
			}
		}
	}
}
