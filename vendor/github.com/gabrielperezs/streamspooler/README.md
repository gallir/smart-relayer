# streamspooler
Clients pooler for AWS Kinesis and Firehose

# Project that us using this package

* https://github.com/gallir/smart-relayer/ - A light and smart cache proxy in Golang that reduces latency in client applications

# Example for Firehose

```golang
fh := firehosePool.New(firehosePool.Config{
		StreamName:    "mystream",
		MaxWorkers:    10,
})
fh.C <- []byte("This a test message")
```