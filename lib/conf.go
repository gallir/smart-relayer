package lib

import (
	"log"
	"net/url"

	"github.com/BurntSushi/toml"
)

const (
	ModeSync        = 0
	ModeSmart       = 1
	responseTimeout = 30
)

type Config struct {
	Comment        string
	GOGC           int //GCPercent
	Relayer        []RelayerConfig
	BufferPoolSize int // If > 0 it will use bybufferpools in redis.Resp if size > BufferPoolSize
}

type RelayerConfig struct {
	Protocol           string // redis | redis2 | redis-cluster | redis-plus | firehose
	Mode               string // smart | sync
	Listen             string // Local url | also is streamName for Kinesis Firehose
	URL                string // Redis/SQS url endpoint
	MaxConnections     int    // Pool management
	MaxIdleConnections int    // Pool management
	Compress           bool
	Uncompress         bool

	UseBufferPool bool // used by the http proxy, enable or diable buffer pool

	//	Parallel           bool // For redis-cluster, send parallel requests
	Pipeline int // If > 0 it does pipelining (buffering)
	Timeout  int // Timeout in seconds to wait for responses from the server

	MaxRecords int    // To send in batch to Kinesis
	Buffer     int    // Size for the channel (queue for Kinesis/Firehose)
	StreamName string // Kinesis/Firehose stream name
	GroupID    string // Group ID for AWS SQS fifo
	Region     string // AWS region
	Profile    string // AWS Profile name
	Concat     bool   // Kinesis/Firehose contact messages, valid just for S3 backend
	Path       string // Path were to store the logs
	S3Bucket   string // S3 Bucket name

	Shards int // Shards for FS plugin
}

func ReadConfig(filename string) (config *Config, err error) {
	var configuration Config
	_, err = toml.DecodeFile(filename, &configuration)
	if err != nil {
		return
	}

	config = &configuration

	for _, r := range config.Relayer {
		if r.Timeout == 0 {
			r.Timeout = responseTimeout
		}
	}

	return
}

// Type return the value of Mode coded in a integer
func (c *RelayerConfig) Type() int {
	if c.Mode == "smart" || c.Mode == "async" {
		return ModeSmart
	}
	return ModeSync
}

func (c *RelayerConfig) Scheme() (scheme string) {
	u, err := url.Parse(c.URL)
	if err != nil {
		log.Fatal(err)
	}
	scheme = u.Scheme
	return
}

func (c *RelayerConfig) Host() (host string) {
	host, err := Host(c.URL)
	if err != nil {
		log.Fatal(err)
	}
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

func Host(s string) (host string, err error) {
	u, err := url.Parse(s)
	host = u.Host
	return
}
