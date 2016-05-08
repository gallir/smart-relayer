package lib

type Relayer interface {
	//New(*RelayerConfig, chan bool) Relayer
	Serve() error
	Port() int
}
