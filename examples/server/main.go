package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/netrusov/easyraft"
	"github.com/netrusov/easyraft/fsm"
)

func ListenAndServe(httpPort int, node *easyraft.Node, stopCh chan any) {
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
