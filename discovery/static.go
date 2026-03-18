package discovery

import "sync"

type StaticDiscovery struct {
	Peers []string

	mu          sync.Mutex
	discoveryCh chan string
	done        chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

func NewStaticDiscovery(peers []string) DiscoveryMethod {
	return &StaticDiscovery{
		Peers: peers,
	}
}

func (d *StaticDiscovery) SupportsNodeAutoRemoval() bool {
	return false
}

func (d *StaticDiscovery) Start(_ string, _ int) (<-chan string, error) {
	out := make(chan string)
	done := make(chan struct{})

	d.mu.Lock()
	d.discoveryCh = out
	d.done = done
	d.stopOnce = sync.Once{}
	d.mu.Unlock()

	d.wg.Go(func() {
		defer close(out)

		for _, peer := range d.Peers {
			select {
			case out <- peer:
			case <-done:
				return
			}
		}
	})

	return out, nil
}

func (d *StaticDiscovery) Stop() error {
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
