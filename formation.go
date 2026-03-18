package easyraft

import (
	"context"
	"time"
)

func (n *Node) evaluateBootstrap(ctx context.Context, discoveryChan <-chan string) ([]string, bool, error) {
	timer := time.NewTimer(n.formationTimeout)
	defer timer.Stop()

	knownPeers := make([]string, 0)
	seenPeers := make(map[string]struct{})
	smallestNodeID := n.ID

	for {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-timer.C:
			return knownPeers, n.ID == smallestNodeID, nil
		case peer, ok := <-discoveryChan:
			if !ok {
				return knownPeers, n.ID == smallestNodeID, nil
			}
			if _, ok := seenPeers[peer]; ok {
				continue
			}

			seenPeers[peer] = struct{}{}
			knownPeers = append(knownPeers, peer)

			peerDetails, err := getPeerDetails(peer)
			if err != nil {
				continue
			}
			if peerDetails.ServerID == n.ID {
				continue
			}
			if peerDetails.HasExistingState {
				return knownPeers, false, nil
			}
			if peerDetails.ServerID != "" && peerDetails.ServerID < smallestNodeID {
				smallestNodeID = peerDetails.ServerID
			}
		}
	}
}

func (n *Node) replayDiscoveredPeers(bufferedPeers []string, discoveryChan <-chan string) <-chan string {
	if len(bufferedPeers) == 0 {
		return discoveryChan
	}

	out := make(chan string)
	go func() {
		defer close(out)

		seenPeers := make(map[string]struct{}, len(bufferedPeers))
		for _, peer := range bufferedPeers {
			if _, ok := seenPeers[peer]; ok {
				continue
			}
			seenPeers[peer] = struct{}{}
			out <- peer
		}

		for peer := range discoveryChan {
			if _, ok := seenPeers[peer]; ok {
				continue
			}
			seenPeers[peer] = struct{}{}
			out <- peer
		}
	}()

	return out
}
