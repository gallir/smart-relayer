package redis

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

// Conn keeps the status of the connection to a server
type RedisIO struct {
	NetBuf   *lib.Netbuf
	Database int
}

const (
	maxBufCount = 1000 // To protect for very large buffer consuming lot of memory
)

func NewConn(netConn net.Conn, readTimeout, writeTimeout time.Duration) *RedisIO {
	cn := &RedisIO{
		NetBuf: lib.NewNetbuf(netConn, readTimeout, writeTimeout),
	}
	return cn
}

func (cn *RedisIO) IsStale(timeout time.Duration) bool {
	return cn.NetBuf.IsStale(timeout)
}

func (cn *RedisIO) Write(b []byte) (int, error) {
	return cn.NetBuf.Write(b)
}

func (cn *RedisIO) Close() error {
	return cn.NetBuf.Close()
}

func (cn *RedisIO) Read(r *Request, parseCommand bool) ([]byte, error) {
	line, err := cn.NetBuf.ReadLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		lib.Debugf("Empty line")
		return nil, malformed("short response line", string(line))
	}

	if r.Buffer == nil {
		r.Buffer = new(bytes.Buffer)
	}

	switch line[0] {
	case '+', '-', ':':
		r.Buffer.Write(line)
		return line, nil
	case '$':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return nil, err
		}
		r.Buffer.Write(line)
		if n > 0 {
			b, err := cn.NetBuf.ReadN(n + 2)
			if err != nil {
				return nil, err
			}
			// Now check for trailing CR
			if b[len(b)-2] != '\r' || b[len(b)-1] != '\n' {
				return nil, malformedMissingCRLF()
			}
			if parseCommand {
				if len(r.Command) == 0 {
					r.Command = bytes.ToUpper(b[:len(b)-2])
				} else {
					if bytes.Compare(r.Command, selectCommand) == 0 {
						n, err = strconv.Atoi(string(b[0 : len(b)-2]))
						if err == nil {
							cn.Database = n
						}
					}
				}
			}
			r.Buffer.Write(b)
		}
		return r.Command, nil
	case '*':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if n < 0 || err != nil {
			return nil, err
		}
		r.Buffer.Write(line)
		for i := 0; i < n; i++ {
			_, err := cn.Read(r, parseCommand)
			if err != nil {
				return nil, malformed("*<numberOfArguments>", string(line))
			}
		}
		return r.Command, nil
	default:
		// Inline request
		r.Buffer.Write(line)
		parts := bytes.Split(line, []byte(" "))
		if len(parts) > 0 {
			r.Command = bytes.ToUpper(bytes.TrimSpace(parts[0]))
		}
		return line, nil
	}
}

func malformed(expected string, got string) error {
	lib.Debugf("Mailformed request:'%s does not match %s\\r\\n'", got, expected)
	return fmt.Errorf("Mailformed request:'%s does not match %s\\r\\n'", got, expected)
}

func malformedLength(expected int, got int) error {
	return fmt.Errorf(
		"Mailformed request: argument length '%d does not match %d\\r\\n'",
		got, expected)
}

func malformedMissingCRLF() error {
	return fmt.Errorf("Mailformed request: line should end with \\r\\n")
}
