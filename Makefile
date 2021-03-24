default: build

build: $(MAINGOPROTOFILES) $(APIGOPROTOFILES)
	go  build -o smart-relayer -ldflags -s 

#	go  build -o smart-relayer  -tags=shortlivedpool -ldflags -s 
build-for-windows:
	GOOS=windows GOARCH=386 go build -o smart-relayer.exe -ldflags '-w -s'

build-arm:
	env GOOS=linux GOARCH=arm64 go build -o smart-relayer-arm -ldflags '-w -s'

clean:
	go clean

