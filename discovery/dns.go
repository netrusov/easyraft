package discovery

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	defaultDNSName = "easyraft"
)

type DNSDiscovery struct {
	dnsName   string
	port      int
	delayTime time.Duration

	mu          sync.Mutex
	discoveryCh chan string
	done        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

func NewDNSDiscovery(dnsName string, port int) DiscoveryMethod {
	delayTime := time.Duration(rand.Intn(5)+1) * time.Second

	if dnsName == "" {
		dnsName = defaultDNSName
	}

	return &DNSDiscovery{
		dnsName:   dnsName,
		port:      port,
		delayTime: delayTime,
	}
}

func (d *DNSDiscovery) Start(_ string, _ int) (<-chan string, error) {
	out := make(chan string)
	done := make(chan struct{})

	d.mu.Lock()
	d.discoveryCh = out
	d.done = done
	d.stopOnce = sync.Once{}
	d.mu.Unlock()

	d.wg.Add(1)
	go d.discovery(out, done)

	return out, nil
}

func (d *DNSDiscovery) Stop() error {
	d.mu.Lock()
	done := d.done
	d.mu.Unlock()

	if done == nil {
		return nil
	}

	d.stopOnce.Do(func() {
		close(done)
	})
	d.wg.Wait()

	d.mu.Lock()
	d.discoveryCh = nil
	d.done = nil
	d.mu.Unlock()

	return nil
}

func (d *DNSDiscovery) SupportsNodeAutoRemoval() bool {
	return false
}

func (d *DNSDiscovery) discovery(out chan string, done <-chan struct{}) {
	defer d.wg.Done()
	defer close(out)

	ticker := time.NewTicker(d.delayTime)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}

		ips, err := net.LookupHost(d.dnsName)
		if err != nil {
			log.Printf("DNS lookup failed for %s: %v\n", d.dnsName, err)
		}

		for _, ip := range ips {
			select {
			case out <- net.JoinHostPort(ip, fmt.Sprintf("%d", d.port)):
			case <-done:
				return
			}
		}
	}
}
