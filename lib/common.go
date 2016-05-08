package lib

type Relayer interface {
	//New(*RelayerConfig, chan bool) Relayer
	Start() error
	Reload(*RelayerConfig)
	Protocol() string
	Port() int
}
