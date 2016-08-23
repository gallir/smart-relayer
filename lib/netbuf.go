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
	writeBuf     *bufio.Writer
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
	nb.readBuf = bufio.NewReader(conn)
	nb.writeBuf = bufio.NewWriter(conn)
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
	return nb.readBuf.Read(b)
}

// Write complies with io.Writer interface
func (nb *Netbuf) Write(b []byte) (n int, e error) {
	nb.usedAt = time.Now()
	if nb.writeTimeout != 0 {
		nb.conn.SetWriteDeadline(nb.usedAt.Add(nb.writeTimeout))
	} else {
		nb.conn.SetWriteDeadline(noDeadline)
	}
	n, e = nb.writeBuf.Write(b)

	if e != nil {
		Debugf("Netbuf, error in write %s", e)
	}
	nb.Flush()
	return
}

func (nb *Netbuf) Flush() error {
	return nb.writeBuf.Flush()
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
