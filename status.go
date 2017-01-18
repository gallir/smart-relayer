package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gallir/smart-relayer/lib"
)

type statusResponse struct {
	TotalRelayers int `json:"totalRelayers"`
	Relayers      []lib.RelayerStatus
}

func runStatus() {
	http.HandleFunc("/", getStatus)                 // set router
	err := http.ListenAndServe(statusListener, nil) // set listen port
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func getStatus(w http.ResponseWriter, r *http.Request) {

	s := &statusResponse{
		TotalRelayers: totalRelayers, // global variable
	}

	if len(relayers) >= 0 {
		for relayerName := range relayers {
			s.Relayers = append(s.Relayers, relayers[relayerName].Status())
		}
	}

	sb, _ := json.Marshal(s)
	fmt.Fprintf(w, string(sb)) // send data to client side
}
