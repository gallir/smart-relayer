package tools

import (
	"log"

	"github.com/BurntSushi/toml"
)

type Config struct {
	Comment string
	Relayer []RelayerConfig
}

type RelayerConfig struct {
	Protocol string
	Type     string
	Listen   int
	Host     string
	Port     int
}

func ReadConfig(filename string, config *Config) {
	_, err := toml.DecodeFile(filename, config)
	if err != nil {
		log.Fatal(err)
	}
}
