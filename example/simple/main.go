package main

import "github.com/gallir/go-bulk-relayer/redis"

func main() {
	server, err := redis.NewServer(redis.DefaultConfig())
	if err != nil {
		panic(err)
	}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
