package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ksrichard/easyraft"
	"github.com/ksrichard/easyraft/discovery"
	"github.com/ksrichard/easyraft/fsm"
	"github.com/ksrichard/easyraft/serializer"
)

func main() {
	raftPort, _ := strconv.Atoi(os.Getenv("EASYRAFT_PORT"))
	discoveryPort, _ := strconv.Atoi(os.Getenv("DISCOVERY_PORT"))
	httpPort, _ := strconv.Atoi(os.Getenv("HTTP_PORT"))
	dataDir := os.Getenv("DATA_DIR")
	dnsName := os.Getenv("DNS_NAME")

	log.Println(dataDir)

	node, err := easyraft.NewNode(
		raftPort,
		discoveryPort,
		dataDir,
		[]fsm.FSMService{fsm.NewInMemoryMapService()},
		serializer.NewMsgPackSerializer(),
		discovery.NewDNSDiscovery(dnsName, raftPort),
		false,
	)
	if err != nil {
		panic(err)
	}
	stopCh, err := node.Start()
	if err != nil {
		panic(err)
	}
	defer node.Stop()

	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		mapName := r.URL.Query().Get("map")
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		result, err := node.RaftApply(fsm.MapPutRequest{
			MapName: mapName,
			Key:     key,
			Value:   value,
		}, time.Second)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		mapName := r.URL.Query().Get("map")
		key := r.URL.Query().Get("key")

		result, err := node.RaftApply(fsm.MapGetRequest{
			MapName: mapName,
			Key:     key,
		}, time.Second)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	server := &http.Server{
		Addr: fmt.Sprintf(":%d", httpPort),
	}
	go func() {
		log.Printf("HTTP server listening on port %d", httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	<-done
	server.Shutdown(context.Background())
	<-stopCh
}
