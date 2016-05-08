package lib

import (
	"log"
	"strconv"
	"strings"

	"net/url"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Comment string
	Relayer []RelayerConfig
}

type RelayerConfig struct {
	Protocol string
	Mode     string
	Listen   int
	Url      string
}

func ReadConfig(filename string) (config *Config, err error) {
	var configuration Config
	_, err = toml.DecodeFile(filename, &configuration)
	if err == nil {
		config = &configuration
	}
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

func (c *RelayerConfig) Port() (port int) {
	host := c.Host()
	hostPort := strings.Split(host, ":")
	if len(hostPort) > 1 {
		port, _ = strconv.Atoi(hostPort[1])
	} else {
		port = 0
	}
	return
}

func (c *RelayerConfig) Proto() (proto string) {
	u, err := url.Parse(c.Url)
	if err != nil || u.Scheme == "" {
		return "tcp"
	}
	return u.Scheme
}
