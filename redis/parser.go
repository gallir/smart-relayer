package redis

import "fmt"

var commands map[string][]byte

func getSelect(n int) []byte {
	str := fmt.Sprintf("%d", n)
	return []byte(fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", len(str), str))
}

func init() {
	commands = map[string][]byte{
		"PING":   []byte("+PONG\r\n"),
		"SET":    []byte("+OK\r\n"),
		"SETEX":  []byte("+OK\r\n"),
		"PSETEX": []byte("+OK\r\n"),
		"MSET":   []byte("+OK\r\n"),
		"HMSET":  []byte("+OK\r\n"),

		"SELECT": []byte("+OK\r\n"),

		"DEL":       []byte(":1\r\n"),
		"HSET":      []byte(":1\r\n"),
		"HDEL":      []byte(":1\r\n"),
		"EXPIRE":    []byte(":1\r\n"),
		"EXPIREAT":  []byte(":1\r\n"),
		"PEXPIRE":   []byte(":1\r\n"),
		"PEXPIREAT": []byte(":1\r\n"),
	}
}
