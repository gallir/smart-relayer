package main

import (
	"errors"
	"fmt"
	"log"
	"log/syslog"
	"os"
	"os/signal"
	"syscall"

	libdebug "runtime/debug"

	"sync"

	"github.com/gallir/radix.improved/redis"
	"github.com/gallir/smart-relayer/httpproxy"
	"github.com/gallir/smart-relayer/lib"
	"github.com/gallir/smart-relayer/redis/cluster"
	"github.com/gallir/smart-relayer/redis/fh"
	"github.com/gallir/smart-relayer/redis/fs"
	"github.com/gallir/smart-relayer/redis/kinesis"
	"github.com/gallir/smart-relayer/redis/radix"
	"github.com/gallir/smart-relayer/redis/rsqs"
)

const (
	version = "8.6.0"
)

var (
	mutex          sync.Mutex
	relayers       = make(map[string]lib.Relayer)
	totalRelayers  = 0
	relayersConfig *lib.Config
	done           = make(chan bool)
	reloadSig      = make(chan os.Signal, 1)
	exitSig        = make(chan os.Signal, 1)
)

func getNewServer(conf lib.RelayerConfig) (srv lib.Relayer, err error) {
	switch conf.Protocol {
	case "redis", "redis2":
		srv, err = redis2.New(conf, done)
	case "redis-cluster", "redis-plus":
		srv, err = cluster.New(conf, done)
	case "firehose":
		srv, err = fh.New(conf, done)
	case "kinesis":
		srv, err = kinesis.New(conf, done)
	case "sqs":
		srv, err = rsqs.New(conf, done)
	case "fs":
		srv, err = fs.New(conf, done)
	case "http":
		srv, err = httpproxy.New(conf, done)
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

	// Force a high number of file descriptoir, if possible
	var rLimit syscall.Rlimit
	e := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if e == nil {
		rLimit.Cur = 65536
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}

	// Show version and exit
	if lib.GlobalConfig.ShowVersion {
		fmt.Println("smart-relayer version", version)
		fmt.Printf("Max files %d/%d\n", rLimit.Cur, rLimit.Max)
		os.Exit(0)
	}

	if !lib.GlobalConfig.Debug {
		logwriter, e := syslog.New(syslog.LOG_INFO|syslog.LOG_USER, "smart-relayer")
		if e == nil {
			log.SetFlags(0)
			log.SetOutput(logwriter)
		}
	}

	// Listen for reload signals
	signal.Notify(reloadSig, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2)
	signal.Notify(exitSig, syscall.SIGINT, syscall.SIGKILL, os.Interrupt, syscall.SIGTERM)

	// Reload config
	go func() {
		for {
			_ = <-reloadSig
			startOrReload()
		}
	}()

	// Exit
	go func() {
		for {
			s := <-exitSig
			log.Printf("Signal %d received, exiting", s)
			for _, r := range relayers {
				r.Exit()
			}
		}
	}()

	if !startOrReload() {
		os.Exit(1)
	}

	for i := 0; i < totalRelayers; i++ {
		<-done
	}
	os.Exit(0)
}
