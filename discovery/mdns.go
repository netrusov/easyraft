package discovery

import (
	"context"
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/mdns"
)

const (
	mdnsServiceName = "_easyraft._tcp"
	mdnsDomain      = "local."
)

var voidLogger = log.New(io.Discard, "", 0)

type MDNSDiscovery struct {
	delayTime time.Duration
	logger    *log.Logger

	mu          sync.Mutex
	discoveryCh chan string
	done        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

func NewMDNSDiscovery() DiscoveryMethod {
	delayTime := time.Duration(rand.Intn(5)+1) * time.Second

	return &MDNSDiscovery{
		delayTime: delayTime,
	}
}

func (d *MDNSDiscovery) Start(nodeID string, nodePort int) (<-chan string, error) {
	if d.logger == nil {
		d.logger = log.Default()
	}

	mdnsServer, err := d.exposeMDNS(nodeID, nodePort)
	if err != nil {
		return nil, err
	}

	out := make(chan string)
	done := make(chan struct{})

	d.mu.Lock()
	d.discoveryCh = out
	d.done = done
	d.stopOnce = sync.Once{}
	d.mu.Unlock()

	d.wg.Add(1)
	go d.discovery(out, done, mdnsServer)

	return out, nil
}

func (d *MDNSDiscovery) discovery(out chan string, done <-chan struct{}, mdnsServer *mdns.Server) {
	defer d.wg.Done()
	defer close(out)
	defer mdnsServer.Shutdown()

	entries := make(chan *mdns.ServiceEntry)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			case entry, ok := <-entries:
				if !ok || entry == nil || entry.AddrV4 == nil {
					continue
				}

				addr := net.JoinHostPort(entry.AddrV4.String(), strconv.Itoa(entry.Port))
				select {
				case out <- addr:
				case <-done:
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(d.delayTime)
	defer ticker.Stop()

	params := mdns.DefaultParams(mdnsServiceName)
	params.Domain = mdnsDomain
	params.Entries = entries
	params.Logger = voidLogger

	for {
		if err := mdns.QueryContext(ctx, params); err != nil {
			select {
			case <-done:
				readers.Wait()
				return
			default:
			}
		}

		select {
		case <-done:
			readers.Wait()
			return
		case <-ticker.C:
		}
	}
}

func (d *MDNSDiscovery) exposeMDNS(nodeID string, nodePort int) (*mdns.Server, error) {
	service, err := mdns.NewMDNSService(
		nodeID,
		mdnsServiceName,
		mdnsDomain,
		"",
		nodePort,
		nil,
		[]string{"txtv=0", "lo=1", "la=2"},
	)
	if err != nil {
		return nil, err
	}

	return mdns.NewServer(&mdns.Config{
		Zone:   service,
		Logger: d.logger,
	})
}

func (d *MDNSDiscovery) SupportsNodeAutoRemoval() bool {
	return true
}

func (d *MDNSDiscovery) SetLogger(logger *log.Logger) {
	d.logger = logger
}

func (d *MDNSDiscovery) Stop() error {
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
