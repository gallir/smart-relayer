package lib

import "flag"

type Relayer interface {
	Start() error
	Reload(*RelayerConfig)
	Exit()
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
