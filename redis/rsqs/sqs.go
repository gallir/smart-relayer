package rsqs

import (
	"context"
	"log"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gallir/smart-relayer/lib"
)

func (srv *Server) failure() {
	srv.Lock()
	defer srv.Unlock()

	if srv.reseting || srv.exiting {
		return
	}

	if time.Now().Sub(srv.lastError) > errorsFrame {
		srv.errors = 0
	}

	srv.errors++
	srv.lastError = time.Now()
	log.Printf("SQS: %d errors detected", srv.errors)

	if srv.errors > maxErrors {
		go srv.retry()
	}
}

func (srv *Server) retry() {
	srv.Lock()
	defer srv.Unlock()

	if srv.failing {
		return
	}

	srv.failing = true
	tries := 0

	for {
		if srv.clientsReset() == nil {
			return
		}

		tries++
		log.Printf("SQS ERROR: %d attempts to connect to kinens", tries)

		if tries >= maxConnectionsTries {
			time.Sleep(connectionRetry * 2)
		} else {
			time.Sleep(connectionRetry)
		}
	}
}

func (srv *Server) clientsReset() (err error) {

	if srv.exiting {
		return nil
	}

	srv.reseting = true
	defer func() { srv.reseting = false }()

	lib.Debugf("SQS Reload config to the stream %s listen %s", srv.config.URL, srv.config.Listen)

	var sess *session.Session

	if srv.config.Profile != "" {
		sess, err = session.NewSessionWithOptions(session.Options{Profile: srv.config.Profile})
	} else {
		sess, err = session.NewSession()
	}

	if err != nil {
		log.Printf("SQS ERROR: session: %s", err)

		srv.errors++
		srv.lastError = time.Now()
		return err
	}

	srv.awsSvc = sqs.New(sess, &aws.Config{Region: aws.String(srv.config.Region)})

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	i := &sqs.GetQueueAttributesInput{
		QueueUrl: &srv.config.URL,
	}

	req, _ := srv.awsSvc.GetQueueAttributesRequest(i)
	req.SetContext(ctx)
	if req.Send() != nil {
		log.Printf("SQS ERROR: session: %s: %s", srv.config.URL, err)
		return err
	}

	lib.Debugf("SQS Connected to %s", srv.config.URL)

	srv.lastConnection = time.Now()
	srv.errors = 0
	srv.failing = false

	currClients := len(srv.clients)

	// No changes in the number of clients
	if currClients == srv.config.MaxConnections {
		return nil
	}

	// If the config define lower number than the active clients remove the difference
	if currClients > srv.config.MaxConnections {
		for i := currClients; i > srv.config.MaxConnections; i-- {
			k := i - 1
			srv.clients[k].Exit()
			srv.clients = append(srv.clients[:k], srv.clients[k+1:]...)
		}
		atomic.StoreInt64(&clientCount, int64(len(srv.clients)))
	} else {
		// If the config define higher number than the active clients start new clients
		for i := currClients; i < srv.config.MaxConnections; i++ {
			srv.clients = append(srv.clients, NewClient(srv))
		}
	}
	atomic.StoreInt64(&clientCount, int64(len(srv.clients)))

	return nil
}
