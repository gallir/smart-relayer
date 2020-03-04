package main

import (
	"log"
	"log/syslog"
)

func initLogging(logName string) {
	logwriter, e := syslog.New(syslog.LOG_INFO|syslog.LOG_USER, "smart-relayer")
	if e == nil {
		log.SetFlags(0)
		log.SetOutput(logwriter)
	}
}
