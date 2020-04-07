FROM golang:1.14-alpine as builder

WORKDIR /go/src/github.com/gallir/smart-relayer/
COPY . .
ENV GO_ENABLED=0
RUN go build -o smart-relayer -ldflags -s 

FROM alpine:3.11
    
WORKDIR /app/
COPY --from=builder /go/src/github.com/gallir/smart-relayer/smart-relayer /app/smart-relayer

EXPOSE 9812
ENTRYPOINT ["/app/smart-relayer", "-c", "config/smart-relayer.conf"]
