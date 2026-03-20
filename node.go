package easyraft

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/netrusov/easyraft/discovery"
	"github.com/netrusov/easyraft/fsm"
	"github.com/netrusov/easyraft/internal/util"
	"github.com/netrusov/easyraft/serializer"
)

type Node struct {
	ID string

	advertiseAddr string
	advertisePort int

	discoveryPort   int
	discoveryMethod discovery.DiscoveryMethod

	memberlist       *memberlist.Memberlist
	memberlistConfig *memberlist.Config

	dataDir          string
	raft             *raft.Raft
	grpcServer       *grpc.Server
	transportManager *transport.Manager
	serializer       serializer.Serializer
	logger           hclog.Logger
	snapshotEnabled  bool
	hasExistingState bool
	bootstrap        bool
	formationTimeout time.Duration

	mu       sync.Mutex
	started  bool
	stopping atomic.Bool
}

const (
	nodeIDFileName      = "node.id"
	stableStoreFileName = "store.boltdb"
	raftLogCacheSize    = 512
)

// NewNode returns an EasyRaft node
func NewNode(cfg *Config) (*Node, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	if cfg.Logger == nil {
		cfg.Logger = defaultLogger()
	}

	advertiseHost := cfg.AdvertiseAddr

	// resolve which address will be used to announce to members
	if advertiseHost == "" || advertiseHost == "0.0.0.0" || advertiseHost == "::" {
		if cfg.AdvertiseAddrProbeHost != "" {
			ip, err := util.GetOutboundIP(cfg.AdvertiseAddrProbeHost)
			if err != nil {
				return nil, err
			}

			advertiseHost = ip.String()
		}
	}

	advertiseAddr := net.JoinHostPort(advertiseHost, strconv.Itoa(cfg.AdvertisePort))

	if err := os.MkdirAll(cfg.DataDir, os.ModePerm); err != nil {
		return nil, err
	}

	nodeID, err := resolveNodeID(cfg)
	if err != nil {
		return nil, err
	}

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(nodeID)
	raftConf.Logger = cfg.Logger.Named("raft")

	// stable/log/snapshot store config
	stableStoreFile := filepath.Join(cfg.DataDir, stableStoreFileName)

	stableStore, err := raftboltdb.NewBoltStore(stableStoreFile)
	if err != nil {
		return nil, err
	}

	logStore, err := raft.NewLogCache(raftLogCacheSize, stableStore)
	if err != nil {
		return nil, err
	}

	var snapshotStore raft.SnapshotStore
	if !cfg.SnapshotEnabled {
		snapshotStore = raft.NewDiscardSnapshotStore()
	} else {
		// TODO: implement: snapshotStore = NewLogsOnlySnapshotStore(serializer)
		return nil, errors.New("snapshots are not supported at the moment")
	}

	grpcTransport := transport.New(
		raft.ServerAddress(advertiseAddr),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	)

	sm := fsm.NewRoutingFSM(cfg.Services)
	sm.Init(cfg.Serializer, standardLogger(cfg.Logger.Named("fsm")))

	memberlistConfig := memberlist.DefaultLANConfig()
	memberlistConfig.BindPort = cfg.DiscoveryPort
	memberlistConfig.Name = fmt.Sprintf("%s:%d", nodeID, cfg.AdvertisePort)
	memberlistConfig.Logger = standardLogger(cfg.Logger.Named("memberlist"))

	hasExistingState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, err
	}

	raftServer, err := raft.NewRaft(
		raftConf,
		sm,
		logStore,
		stableStore,
		snapshotStore,
		grpcTransport.Transport(),
	)
	if err != nil {
		return nil, err
	}

	cfg.DiscoveryMethod.SetLogger(standardLogger(cfg.Logger.Named("discovery")))

	node := &Node{
		ID: nodeID,

		advertiseAddr: advertiseAddr,
		advertisePort: cfg.AdvertisePort,

		discoveryPort:   cfg.DiscoveryPort,
		discoveryMethod: cfg.DiscoveryMethod,

		memberlistConfig: memberlistConfig,

		dataDir:          cfg.DataDir,
		raft:             raftServer,
		transportManager: grpcTransport,
		serializer:       cfg.Serializer,
		logger:           cfg.Logger.Named("node"),
		snapshotEnabled:  cfg.SnapshotEnabled,
		hasExistingState: hasExistingState,
		bootstrap:        cfg.Bootstrap,
		formationTimeout: cfg.FormationTimeout,
	}

	return node, nil
}

func resolveNodeID(cfg *Config) (string, error) {
	if cfg.NodeID != "" {
		return cfg.NodeID, persistNodeID(cfg.DataDir, cfg.NodeID)
	}

	nodeIDPath := filepath.Join(cfg.DataDir, nodeIDFileName)
	if util.FileExists(nodeIDPath) {
		payload, err := os.ReadFile(nodeIDPath)
		if err != nil {
			return "", err
		}

		nodeID := strings.TrimSpace(string(payload))
		if nodeID == "" {
			return "", errors.New("persisted node ID is empty")
		}
		return nodeID, nil
	}

	nodeID := rand.Text()
	if err := persistNodeID(cfg.DataDir, nodeID); err != nil {
		return "", err
	}

	return nodeID, nil
}

func persistNodeID(dataDir, nodeID string) error {
	nodeIDPath := filepath.Join(dataDir, nodeIDFileName)
	if util.FileExists(nodeIDPath) {
		payload, err := os.ReadFile(nodeIDPath)
		if err != nil {
			return err
		}

		existingNodeID := strings.TrimSpace(string(payload))
		if existingNodeID != "" && existingNodeID != nodeID {
			return fmt.Errorf("persisted node ID %q does not match configured node ID %q", existingNodeID, nodeID)
		}

		if existingNodeID == nodeID {
			return nil
		}
	}

	return os.WriteFile(nodeIDPath, []byte(nodeID), 0o600)
}

// RaftApply is used to apply any new logs to the raft cluster
// this method does automatic forwarding to Leader Node
func (n *Node) RaftApply(request any, timeout time.Duration) (any, error) {
	payload, err := n.serializer.Serialize(request)
	if err != nil {
		return nil, err
	}

	if n.IsLeader() {
		result := n.raft.Apply(payload, timeout)
		if result.Error() != nil {
			return nil, result.Error()
		}

		switch result.Response().(type) {
		case error:
			return nil, result.Response().(error)
		default:
			return result.Response(), nil
		}
	}

	response, err := applyOnLeader(n, payload)
	if err != nil {
		return nil, err
	}
	return response, nil
}
