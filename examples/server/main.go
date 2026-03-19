package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/netrusov/easyraft"
	"github.com/netrusov/easyraft/fsm"
)

func ListenAndServe(ctx context.Context, httpPort int, node *easyraft.Node) error {
	leaderCh := node.LeaderCh()
	var isLeader atomic.Bool

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-leaderCh:
				if !ok {
					log.Println("leadership channel closed")
					return
				}
				isLeader.Store(val)
			}
		}
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
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
		w.Header().Set("x-easyraft-leader", strconv.FormatBool(isLeader.Load()))

		json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
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
		w.Header().Set("x-easyraft-leader", strconv.FormatBool(isLeader.Load()))

		json.NewEncoder(w).Encode(result)
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", httpPort),
		Handler: mux,
	}
	go func() {
		log.Printf("HTTP server listening on port %d", httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server stopped with error: %v", err)
		}
	}()

	<-ctx.Done()
	return server.Shutdown(context.Background())
}
