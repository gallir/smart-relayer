default: build

build: $(MAINGOPROTOFILES) $(APIGOPROTOFILES)
	go  build -o smart-relayer -ldflags -s 


clean:
	go clean

