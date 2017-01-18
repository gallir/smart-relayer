package lib

import (
	"bufio"
	"net"
	"time"
)

var NoDeadline = time.Time{}

// NetBuffedReadWriter is a reader-writer buffered net connection
type NetBuffedReadWriter struct {
	conn         net.Conn
	buf          *bufio.ReadWriter
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func NewNetReadWriter(conn net.Conn, readTimeout, writeTimeout time.Duration) *NetBuffedReadWriter {
	nb := &NetBuffedReadWriter{
		conn:         conn,
		readTimeout:  readTimeout, // We use different read timeouts for the server and local client
		writeTimeout: writeTimeout,
	}
	nb.buf = bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	return nb
}

// Read complies with io.Reader interface
func (nb *NetBuffedReadWriter) Read(b []byte) (n int, e error) {
	if nb.readTimeout == 0 {
		return nb.buf.Read(b)
	}

	nb.conn.SetReadDeadline(time.Now().Add(nb.readTimeout))
	n, e = nb.buf.Read(b)
	if e == nil {
		nb.conn.SetReadDeadline(NoDeadline)
	}
	return
}

// Write complies with io.Writer interface
func (nb *NetBuffedReadWriter) Write(b []byte) (n int, e error) {
	return nb.buf.Write(b)
}

func (nb *NetBuffedReadWriter) Flush() (e error) {
	if nb.writeTimeout == 0 {
		return nb.buf.Flush()
	}

	nb.conn.SetWriteDeadline(time.Now().Add(nb.writeTimeout))
	e = nb.buf.Flush()
	if e == nil {
		nb.conn.SetWriteDeadline(NoDeadline)
	}
	return
}
