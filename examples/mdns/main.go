package main

import (
	"os"
	"strconv"

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

	_, err = node.Start()
	if err != nil {
		panic(err)
	}
	defer node.Stop()

	server.ListenAndServe(httpPort, node)
}
