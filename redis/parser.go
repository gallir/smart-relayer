package redis

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/gallir/smart-relayer/lib"
)

// RedisIO keeps the status of the connection to a server
type Parser struct {
	netBuf   *lib.Netbuf
	database int
}

const (
	maxBufCount = 1000 // To protect for very large buffer consuming lot of memory
)

func newParser(netConn net.Conn, readTimeout, writeTimeout time.Duration) *Parser {
	p := &Parser{
		netBuf: lib.NewNetbuf(netConn, readTimeout, writeTimeout),
	}
	return p
}

func (p *Parser) isStale(timeout time.Duration) bool {
	return p.netBuf.IsStale(timeout)
}

func (p *Parser) Write(b []byte) (int, error) {
	return p.netBuf.Write(b)
}

func (p *Parser) close() error {
	return p.netBuf.Close()
}

func (p *Parser) read(r *Request, parseCommand bool) ([]byte, error) {
	line, err := p.netBuf.ReadLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		lib.Debugf("Empty line")
		return nil, malformed("short response line", string(line))
	}

	if r.buffer == nil {
		r.buffer = new(bytes.Buffer)
	}

	switch line[0] {
	case '+', '-', ':':
		r.buffer.Write(line)
		return line, nil
	case '$':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if err != nil {
			return nil, err
		}
		r.buffer.Write(line)
		if n > 0 {
			b, err := p.netBuf.ReadN(n + 2)
			if err != nil {
				return nil, err
			}
			// Now check for trailing CR
			if b[len(b)-2] != '\r' || b[len(b)-1] != '\n' {
				return nil, malformedMissingCRLF()
			}
			if parseCommand {
				if len(r.command) == 0 {
					r.command = bytes.ToUpper(b[:len(b)-2])
				} else {
					if bytes.Compare(r.command, selectCommand) == 0 {
						n, err = strconv.Atoi(string(b[0 : len(b)-2]))
						if err == nil {
							p.database = n
						}
					}
				}
			}
			r.buffer.Write(b)
		}
		return r.command, nil
	case '*':
		n, err := strconv.Atoi(string(line[1 : len(line)-2]))
		if n < 0 || err != nil {
			return nil, err
		}
		r.buffer.Write(line)
		for i := 0; i < n; i++ {
			_, err := p.read(r, parseCommand)
			if err != nil {
				return nil, malformed("*<numberOfArguments>", string(line))
			}
		}
		return r.command, nil
	default:
		// Inline request
		r.buffer.Write(line)
		parts := bytes.Split(line, []byte(" "))
		if len(parts) > 0 {
			r.command = bytes.ToUpper(bytes.TrimSpace(parts[0]))
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
