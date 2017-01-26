package lib

import "flag"

type Relayer interface {
	Start() error
	Reload(*RelayerConfig) error
	Status() RelayerStatus
	Exit()
}

type RelayerStatus struct {
	Listen   string
	Host     string
	Protocol string
	Sync     int64
	Async    int64
	Errors   int64
	Idle     int
	Free     int
	Clients  int
}

type RelayerClient interface {
	IsValid() bool
	Exit()
	Send(r interface{}) error
	Reload(*RelayerConfig)
}

type MainConfig struct {
	ConfigFileName string
	Debug          bool
	ShowVersion    bool
}

// GlobalConfig is the configuration for the main programm
var GlobalConfig MainConfig

func init() {
	flag.StringVar(&GlobalConfig.ConfigFileName, "c", "relayer.conf", "Configuration filename")
	flag.BoolVar(&GlobalConfig.Debug, "d", false, "Show debug info")
	flag.BoolVar(&GlobalConfig.ShowVersion, "v", false, "Show version and exit")
	flag.Parse()
}
