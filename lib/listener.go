package lib

import (
	"log"
	"net"
	"os"
)

func Listener(c *RelayerConfig) (net.Listener, error) {
	connType := c.ListenScheme()
	addr := c.ListenHost()

	// Check that the socket does not exist
	if connType == "unix" {
		if s, err := os.Stat(addr); err == nil {
			if (s.Mode() & os.ModeSocket) > 0 {
				// Remove existing socket
				// log.Println("Warning, removing existing socket", addr)
				os.Remove(addr)
			} else {
				log.Println("socket", addr, s.Mode(), os.ModeSocket)
				log.Fatalf("Socket %s exists and it's not a Unix socket", addr)
			}
		}
	}

	listener, e := net.Listen(connType, addr)
	if e != nil {
		log.Println("Error listening to", addr, e)
		return nil, e
	}

	if connType == "unix" {
		// Make sure is accesible for everyone
		os.Chmod(addr, 0777)
	}

	log.Printf("Starting redis server at %s for target %s", addr, c.Host())
	return listener, nil

}
