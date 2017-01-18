package lib

import (
	"log"
	"net"
	"os"
)

type Listener struct {
	config   RelayerConfig
	listener net.Listener
}

// MewListener check sockets and files and return a listener alread listening
func NewListener(c RelayerConfig) (l *Listener, e error) {
	l = &Listener{
		config: c,
	}

	l.clean()

	connType := c.ListenScheme()
	addr := c.ListenHost()

	l.listener, e = net.Listen(connType, addr)
	if e != nil {
		l.listener = nil
		log.Println("Error listening to", addr, e)
		return nil, e
	}

	if connType == "unix" {
		// Make sure is accesible for everyone
		os.Chmod(addr, 0777)
	}

	log.Printf("Starting listening at %s for target %s", addr, c.Host())
	return l, nil

}

func (l *Listener) Close() error {
	if l.listener != nil {
		e := l.listener.Close()
		l.listener = nil
		return e
	}
	return nil
}

func (l *Listener) Addr() net.Addr {
	if l.listener != nil {
		return l.listener.Addr()
	}
	return nil
}

func (l *Listener) Accept() (net.Conn, error) {
	if l.listener == nil {
		log.Panicln("Trying to accept from a closed listener")
	}
	return l.listener.Accept()
}

func (l *Listener) clean() {
	connType := l.config.ListenScheme()
	addr := l.config.ListenHost()

	// Check that the socket does not exist
	if connType == "unix" {
		if s, err := os.Stat(addr); err == nil {
			if (s.Mode() & os.ModeSocket) > 0 {
				// Remove existing socket
				if l.listener == nil {
					// We are starting but there was a previous socket file
					log.Println("Warning, removing existing socket", addr)
					os.Remove(addr)
				}
			} else {
				log.Println("socket", addr, s.Mode(), os.ModeSocket)
				log.Fatalf("Socket %s exists and it's not a Unix socket", addr)
			}
		}
	}

}
