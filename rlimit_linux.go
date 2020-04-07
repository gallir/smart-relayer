package main

import (
	"fmt"
	"syscall"
)

var rLimit syscall.Rlimit

func setRLimit() {
	// Force a high number of file descriptoir, if possible
	e := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if e == nil {
		rLimit.Cur = 65536
		_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}
}

func showRLimit() {
	fmt.Printf("Max files %d/%d\n", rLimit.Cur, rLimit.Max)
}
