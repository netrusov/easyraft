package discovery

import (
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/mdns"
)

const (
	mdnsServiceName = "_easyraft._tcp"
	mdnsDomain      = "local."
)

type MDNSDiscovery struct {
	delayTime     time.Duration
	nodeID        string
	nodePort      int
	mdnsServer    *mdns.Server
	discoveryChan chan string
	stopChan      chan bool
}

func NewMDNSDiscovery() DiscoveryMethod {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	delayTime := time.Duration(r.Intn(5)+1) * time.Second

	return &MDNSDiscovery{
		delayTime:     delayTime,
		discoveryChan: make(chan string),
		stopChan:      make(chan bool),
	}
}

func (d *MDNSDiscovery) Start(nodeID string, nodePort int) (chan string, error) {
	d.nodeID, d.nodePort = nodeID, nodePort
	if d.discoveryChan == nil {
		d.discoveryChan = make(chan string)
	}

	go d.discovery()

	return d.discoveryChan, nil
}

func (d *MDNSDiscovery) discovery() {
	// expose mdns server
	mdnsServer, err := d.exposeMDNS()
	if err != nil {
		log.Fatal(err)
	}
	d.mdnsServer = mdnsServer

	entries := make(chan *mdns.ServiceEntry)
	go func() {
		for {
			select {
			case <-d.stopChan:
				break
			case entry := <-entries:
				d.discoveryChan <- net.JoinHostPort(entry.AddrV4.String(), strconv.Itoa(entry.Port))
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	for {
		select {
		case <-d.stopChan:
			cancel()
			break
		default:
			params := mdns.DefaultParams(mdnsServiceName)
			params.Domain = mdnsDomain
			params.Entries = entries

			err = mdns.QueryContext(ctx, params)
			if err != nil {
				log.Printf("Error during mDNS lookup: %v\n", err)
			}
			time.Sleep(d.delayTime)
		}
	}
}

func (d *MDNSDiscovery) exposeMDNS() (*mdns.Server, error) {
	service, err := mdns.NewMDNSService(
		d.nodeID,
		mdnsServiceName,
		mdnsDomain,
		"",
		d.nodePort,
		nil,
		[]string{"txtv=0", "lo=1", "la=2"},
	)
	if err != nil {
		return nil, err
	}

	return mdns.NewServer(&mdns.Config{Zone: service})
}

func (d *MDNSDiscovery) SupportsNodeAutoRemoval() bool {
	return true
}

func (d *MDNSDiscovery) Stop() {
	d.stopChan <- true
	d.mdnsServer.Shutdown()
	close(d.discoveryChan)
}
