package lib

import (
	"log"

	"net/url"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Comment string
	Relayer []RelayerConfig
}

type RelayerConfig struct {
	Protocol           string
	Mode               string
	Listen             string
	Url                string
	MaxConnections     int
	MaxIdleConnections int
	Compress           bool
	Uncompress         bool
}

func ReadConfig(filename string) (config *Config, err error) {
	var configuration Config
	_, err = toml.DecodeFile(filename, &configuration)
	if err == nil {
		config = &configuration
	}
	return
}

func (c *RelayerConfig) Scheme() (scheme string) {
	u, err := url.Parse(c.Url)
	if err != nil {
		log.Fatal(err)
	}
	scheme = u.Scheme
	return
}

func (c *RelayerConfig) Host() (host string) {
	u, err := url.Parse(c.Url)
	if err != nil {
		log.Fatal(err)
	}
	host = u.Host
	return
}

func (c *RelayerConfig) ListenScheme() (scheme string) {
	u, err := url.Parse(c.Listen)
	if err != nil {
		log.Fatal(err)
	}
	scheme = u.Scheme
	return
}

func (c *RelayerConfig) ListenHost() (host string) {
	u, err := url.Parse(c.Listen)
	if err != nil {
		log.Fatal(err)
	}
	if u.Host == "" {
		host = u.Path
	} else {
		host = u.Host
	}
	return
}
