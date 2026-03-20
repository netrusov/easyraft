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
)

func main() {
	advertisePort, _ := strconv.Atoi(os.Getenv("ADVERTISE_PORT"))
	discoveryPort, _ := strconv.Atoi(os.Getenv("DISCOVERY_PORT"))
	httpPort, _ := strconv.Atoi(os.Getenv("HTTP_PORT"))
	dataDir := os.Getenv("DATA_DIR")

	cfg := easyraft.DefaultConfig()
	cfg.AdvertisePort = advertisePort
	cfg.DiscoveryPort = discoveryPort
	cfg.DataDir = dataDir
	cfg.DiscoveryMethod = discovery.NewMDNSDiscovery()

	node, err := easyraft.NewNode(cfg)
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
