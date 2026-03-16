package discovery

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

const (
	defaultDNSName = "easyraft"
)

type DNSDiscovery struct {
	dnsName       string
	port          int
	delayTime     time.Duration
	discoveryChan chan string
	stopChan      chan bool
}

func NewDNSDiscovery(dnsName string, port int) DiscoveryMethod {
	delayTime := time.Duration(rand.Intn(5)+1) * time.Second

	if dnsName == "" {
		dnsName = defaultDNSName
	}

	return &DNSDiscovery{
		dnsName:       dnsName,
		port:          port,
		delayTime:     delayTime,
		discoveryChan: make(chan string),
		stopChan:      make(chan bool),
	}
}

func (d *DNSDiscovery) Start(nodeID string, nodePort int) (chan string, error) {
	go d.discovery()

	return d.discoveryChan, nil
}

func (d *DNSDiscovery) Stop() {
	d.stopChan <- true
	close(d.discoveryChan)
}

func (d *DNSDiscovery) SupportsNodeAutoRemoval() bool {
	return false
}

func (d *DNSDiscovery) discovery() {
	ticker := time.NewTicker(d.delayTime)

	for {
		select {
		case <-ticker.C:
			ips, err := net.LookupHost(d.dnsName)
			if err != nil {
				log.Printf("DNS lookup failed for %s: %v\n", d.dnsName, err)
			}

			if len(ips) == 0 {
				continue
			}

			for _, ip := range ips {
				d.discoveryChan <- net.JoinHostPort(ip, fmt.Sprintf("%d", d.port))
			}
		case <-d.stopChan:
			ticker.Stop()
			return
		}
	}

}
