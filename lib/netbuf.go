package lib

import (
	"bufio"
	"io"
	"net"
	"time"
)

// Netbuf is a read buffered net connection
type Netbuf struct {
	conn         net.Conn
	readBuf      *bufio.Reader
	internalBuf  []byte
	readTimeout  time.Duration
	writeTimeout time.Duration
	bufCount     int
	usedAt       time.Time
}

const (
	maxBufCount = 1000 // To protect for very large buffer consuming lot of memory
)

var noDeadline = time.Time{}

func NewNetbuf(conn net.Conn, readTimeout, writeTimeout time.Duration) *Netbuf {
	nb := &Netbuf{
		conn:         conn,
		usedAt:       time.Now(),
		readTimeout:  readTimeout, // We use different read timeouts for the server and local client
		writeTimeout: writeTimeout,
	}
	nb.readBuf = bufio.NewReader(nb)
	return nb
}

func (nb *Netbuf) IsStale(timeout time.Duration) bool {
	return timeout > 0 && time.Since(nb.usedAt) > timeout
}

// Read complies with io.Reader interface
func (nb *Netbuf) Read(b []byte) (int, error) {
	nb.usedAt = time.Now()
	if nb.readTimeout != 0 {
		nb.conn.SetReadDeadline(nb.usedAt.Add(nb.readTimeout))
	} else {
		nb.conn.SetReadDeadline(noDeadline)
	}
	return nb.conn.Read(b)
}

// Write complies with io.Writer interface
func (nb *Netbuf) Write(b []byte) (int, error) {
	nb.usedAt = time.Now()
	if nb.writeTimeout != 0 {
		nb.conn.SetWriteDeadline(nb.usedAt.Add(nb.writeTimeout))
	} else {
		nb.conn.SetWriteDeadline(noDeadline)
	}
	return nb.conn.Write(b)
}

func (nb *Netbuf) RemoteAddr() net.Addr {
	return nb.conn.RemoteAddr()
}

func (nb *Netbuf) ReadLine() ([]byte, error) {
	return nb.readBuf.ReadBytes('\n')
}

func (nb *Netbuf) ReadN(n int) ([]byte, error) {
	if nb.bufCount > maxBufCount || cap(nb.internalBuf) < n {
		nb.internalBuf = make([]byte, n)
		nb.bufCount = 0
	} else {
		nb.internalBuf = nb.internalBuf[:n]
		nb.bufCount++
	}
	_, err := io.ReadFull(nb.readBuf, nb.internalBuf)
	return nb.internalBuf, err
}

func (nb *Netbuf) Close() error {
	return nb.conn.Close()
}
