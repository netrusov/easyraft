package discovery

import "log"

// DiscoveryMethod gives the interface to perform automatic Node discovery
type DiscoveryMethod interface {
	SetLogger(logger *log.Logger)

	// SupportsNodeAutoRemoval indicates whether the actual discovery method supports the automatic node removal or not
	SupportsNodeAutoRemoval() bool

	// Start is about to start the discovery method
	// it returns a channel where the node will consume node addresses ("IP:NodeAdvertisePort") until the channel gets closed
	Start(nodeID string, nodePort int) (<-chan string, error)

	// Stop should stop the discovery method and all of its goroutines, it should close discovery channel returned in Start
	Stop() error
}
