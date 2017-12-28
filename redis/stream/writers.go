package stream

import (
	"fmt"
	"log"
	"os"
	"time"
)

type writer struct {
	srv  *Server
	done chan bool

	d        time.Time
	dirName  string
	fileName string
}

func newWriter(srv *Server) *writer {
	w := &writer{
		srv:  srv,
		done: make(chan bool, 1),
	}
	go w.listen()
	return w
}

func (w *writer) listen() {
	for {
		select {
		case m := <-w.srv.C:
			if err := w.writeTo(m); err == nil {
				putMsg(m)
			} else {
				// send message back to the channel
				time.Sleep(5 * time.Second)
				w.srv.C <- m
			}
		case <-w.done:
			if len(w.srv.C) == 0 {
				return
			}
		}
	}
}

func (w *writer) writeTo(m *Msg) error {
	w.dirName = m.path()
	if err := os.MkdirAll(w.dirName, os.ModePerm); err != nil {
		log.Printf("File ERROR: %s", err)
		return err
	}

	w.fileName = fmt.Sprintf("%s/%s", w.dirName, m.filename())

	newFile, err := os.OpenFile(w.fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("File ERROR: %s", err)
		return err
	}
	defer newFile.Close()

	if _, err := m.b.WriteTo(newFile); err != nil {
		log.Printf("File ERROR: writing log: %s", err)
		return err
	}

	return nil
}

func (w *writer) exit() {
	w.done <- true
}
