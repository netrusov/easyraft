package main

import (
	"log"
	"os"
	"strconv"

	"github.com/netrusov/easyraft"
	"github.com/netrusov/easyraft/discovery"
	"github.com/netrusov/easyraft/examples/server"
	"github.com/netrusov/easyraft/fsm"
	"github.com/netrusov/easyraft/serializer"
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
		"dummy",
	)
	if err != nil {
		panic(err)
	}

	stopCh, err := node.Start()
	if err != nil {
		panic(err)
	}
	defer node.Stop()

	server.ListenAndServe(httpPort, node, stopCh)
}
