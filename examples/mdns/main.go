package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/netrusov/easyraft"
	"github.com/netrusov/easyraft/discovery"
	"github.com/netrusov/easyraft/examples/server"
	"github.com/netrusov/easyraft/fsm"
	"github.com/netrusov/easyraft/serializer"
)

func main() {
	// raft details
	raftPort, _ := strconv.Atoi(os.Getenv("EASYRAFT_PORT"))
	discoveryPort, _ := strconv.Atoi(os.Getenv("DISCOVERY_PORT"))
	httpPort, _ := strconv.Atoi(os.Getenv("HTTP_PORT"))
	dataDir := os.Getenv("DATA_DIR")

	// EasyRaft Node
	node, err := easyraft.NewNode(
		raftPort,
		discoveryPort,
		dataDir,
		[]fsm.FSMService{fsm.NewInMemoryMapService()},
		serializer.NewMsgPackSerializer(),
		discovery.NewMDNSDiscovery(),
		false,
		"",
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = node.Start(ctx)
	if err != nil {
		panic(err)
	}

	if err := server.ListenAndServe(ctx, httpPort, node); err != nil {
		panic(err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := node.Stop(shutdownCtx); err != nil {
		panic(err)
	}
}
