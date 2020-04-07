package main

import (
	"golang.org/x/sys/windows/svc/eventlog"
	"log"
)

type WindowsLogger struct {
	logger *eventlog.Log
}

func (w WindowsLogger) Write(b []byte) (int, error) {
	e := w.logger.Info(999, string(b))
	return len(b), e
}

func initLogging(logName string) {

	eventlog.InstallAsEventCreate(logName, eventlog.Error|eventlog.Warning|eventlog.Info)
	l, _ := eventlog.Open(logName)

	windowsLogger := WindowsLogger{
		logger: l,
	}

	log.SetFlags(0)
	log.SetOutput(windowsLogger)
}
