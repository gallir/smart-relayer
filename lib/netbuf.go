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
}

const (
	maxBufCount = 1000 // To protect for very large buffer consuming lot of memory
)

var noDeadline = time.Time{}

func NewNetbuf(conn net.Conn, readTimeout, writeTimeout time.Duration) *Netbuf {
	nb := &Netbuf{
		conn:         conn,
		readTimeout:  readTimeout, // We use different read timeouts for the server and local client
		writeTimeout: writeTimeout,
	}
	nb.readBuf = bufio.NewReader(conn)
	return nb
}

// Read complies with io.Reader interface
func (nb *Netbuf) Read(b []byte) (int, error) {
	if nb.readTimeout != 0 {
		nb.conn.SetReadDeadline(time.Now().Add(nb.readTimeout))
	} else {
		nb.conn.SetReadDeadline(noDeadline)
	}
	return nb.readBuf.Read(b)
}

// Write complies with io.Writer interface
func (nb *Netbuf) Write(b []byte) (n int, e error) {
	if nb.writeTimeout != 0 {
		nb.conn.SetWriteDeadline(time.Now().Add(nb.writeTimeout))
	} else {
		nb.conn.SetWriteDeadline(noDeadline)
	}
	n, e = nb.conn.Write(b)

	if e != nil {
		Debugf("Netbuf, error in write %s", e)
	}
	return
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
