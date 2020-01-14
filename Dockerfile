FROM FROM golang:1.13.0-alpine3.10 as builder

WORKDIR /go/src/github.com/gallir/smart-relayer/

COPY . .
RUN go build -o smart-relayer -ldflags -s 

FROM alpine:3.10
    
WORKDIR /go/app/
COPY --from=builder /go/src/github.com/gallir/smart-relayer/smart-relayer /go/app/smart-relayer

EXPOSE 9812

ENTRYPOINT ["./smart-relayer", "-c", "config/smart-relayer.conf"]
