package lib

import "flag"

type Relayer interface {
	//New(*RelayerConfig, chan bool) Relayer
	Start() error
	Reload(*RelayerConfig)
	Protocol() string
	Listen() string
	Config() *RelayerConfig
	Mode() int
}

type MainConfig struct {
	ConfigFileName string
	Debug          bool
}

// GlobalConfig is the configuration for the main programm
var GlobalConfig MainConfig

func init() {
	flag.StringVar(&GlobalConfig.ConfigFileName, "c", "relayer.conf", "Configuration filename")
	flag.BoolVar(&GlobalConfig.Debug, "d", false, "Show debug info")
	flag.Parse()
}
