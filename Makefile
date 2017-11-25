default: build

build: $(MAINGOPROTOFILES) $(APIGOPROTOFILES)
	go  build -o smart-relayer -ldflags -s 

#	go  build -o smart-relayer  -tags=shortlivedpool -ldflags -s 


clean:
	go clean

