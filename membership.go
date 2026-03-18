package easyraft

import (
	"fmt"
	"strings"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
)

// handleDiscoveredNodes handles the discovered Node additions
func (n *Node) handleDiscoveredNodes(discoveryChan <-chan string) {
	for peer := range discoveryChan {
		details, err := getPeerDetails(peer)
		if err != nil {
			continue
		}

		serverID := details.ServerID
		needToAddNode := true
		for _, server := range n.raft.GetConfiguration().Configuration().Servers {
			if server.ID == raft.ServerID(serverID) || string(server.Address) == peer {
				needToAddNode = false
				break
			}
		}

		if needToAddNode && n.memberlist != nil {
			peerHost := strings.Split(peer, ":")[0]
			peerDiscoveryAddr := fmt.Sprintf("%s:%d", peerHost, details.DiscoveryPort)
			if _, err = n.memberlist.Join([]string{peerDiscoveryAddr}); err != nil {
				n.logger.Error("failed to join cluster using discovery address", "address", peerDiscoveryAddr, "error", err)
			}
		}
	}
}

// NotifyJoin triggered when a new Node has been joined to the cluster (discovery only)
// and capable of joining the Node to the raft cluster
func (n *Node) NotifyJoin(node *memberlist.Node) {
	if !n.isLeader() {
		return
	}

	nameParts := strings.Split(node.Name, ":")
	nodeID, nodePort := nameParts[0], nameParts[1]
	nodeAddr := fmt.Sprintf("%s:%s", node.Addr, nodePort)
	result := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(nodeAddr), 0, 0)

	if result.Error() != nil {
		n.logger.Error("failed to add voter", "node_id", nodeID, "address", nodeAddr, "error", result.Error())
	}
}

// NotifyLeave triggered when a Node becomes unavailable after a period of time
// it will remove the unavailable Node from the Raft cluster
func (n *Node) NotifyLeave(node *memberlist.Node) {
	if !n.discoveryMethod.SupportsNodeAutoRemoval() {
		return
	}

	if !n.isLeader() {
		return
	}

	nodeID := strings.Split(node.Name, ":")[0]
	result := n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)

	err := result.Error()
	if err != nil {
		n.logger.Error("failed to remove raft node", "node_id", nodeID, "error", err)
	}
}

func (n *Node) NotifyUpdate(_ *memberlist.Node) {
}

func (n *Node) isLeader() bool {
	if err := n.raft.VerifyLeader().Error(); err != nil {
		return false
	}

	return true
}
