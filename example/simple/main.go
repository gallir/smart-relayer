package main

import relayer "github.com/gallir/go-bulk-relayer"

func main() {
	server, err := relayer.NewServer(relayer.DefaultConfig())
	if err != nil {
		panic(err)
	}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}
