package easyraft

import (
	"context"
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
	"github.com/zemirco/uid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/netrusov/easyraft/discovery"
	"github.com/netrusov/easyraft/fsm"
	ergrpc "github.com/netrusov/easyraft/grpc"
	"github.com/netrusov/easyraft/serializer"
	"github.com/netrusov/easyraft/util"
)

type Node struct {
	ID               string
	RaftPort         int
	DiscoveryPort    int
	address          string
	dataDir          string
	Raft             *raft.Raft
	GrpcServer       *grpc.Server
	DiscoveryMethod  discovery.DiscoveryMethod
	TransportManager *transport.Manager
	Serializer       serializer.Serializer
	memberlist       *memberlist.Memberlist
	memberlistConfig *memberlist.Config
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
	nodeIDFileName = "node.id"

	defaultRaftPort         = 5000
	defaultDiscoveryPort    = 5001
	defaultDataDir          = "erdb"
	defaultRaftLogCacheSize = 512
	defaultRaftLogLevel     = "Info"
	defaultFormationTimeout = 5 * time.Second
)

type Config struct {
	NodeID               string
	RaftPort             int
	DiscoveryPort        int
	DataDir              string
	Services             []fsm.FSMService
	Serializer           serializer.Serializer
	DiscoveryMethod      discovery.DiscoveryMethod
	SnapshotEnabled      bool
	Bootstrap            bool
	FormationTimeout     time.Duration
	ResolveAdvertiseAddr string
	Logger               hclog.Logger
}

func DefaultConfig() *Config {
	return &Config{
		RaftPort:         defaultRaftPort,
		DiscoveryPort:    defaultDiscoveryPort,
		DataDir:          defaultDataDir,
		Services:         []fsm.FSMService{fsm.NewInMemoryMapService()},
		Serializer:       serializer.NewMsgPackSerializer(),
		Logger:           defaultLogger(),
		Bootstrap:        false,
		FormationTimeout: defaultFormationTimeout,
	}
}

func ValidateConfig(cfg *Config) error {
	if cfg == nil {
		return errors.New("config is required")
	}
	if cfg.Serializer == nil {
		return errors.New("serializer is required")
	}
	if cfg.DiscoveryMethod == nil {
		return errors.New("discovery method is required")
	}

	return nil
}

// NewNode returns an EasyRaft node
func NewNode(cfg *Config) (*Node, error) {
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}

	advertiseAddr := "0.0.0.0"

	// resolve which address will be used to announce to members
	if cfg.ResolveAdvertiseAddr != "" {
		advertiseAddr = util.GetOutboundIP(cfg.ResolveAdvertiseAddr).String()
	}

	// default raft config
	addr := net.JoinHostPort(advertiseAddr, strconv.Itoa(cfg.RaftPort))
	if err := os.MkdirAll(cfg.DataDir, os.ModePerm); err != nil {
		return nil, err
	}

	nodeID, err := resolveNodeID(cfg)
	if err != nil {
		return nil, err
	}

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(nodeID)
	raftLogCacheSize := defaultRaftLogCacheSize
	raftConf.LogLevel = defaultRaftLogLevel

	raftConf.Logger = cfg.Logger.Named("raft")

	// stable/log/snapshot store config
	stableStoreFile := filepath.Join(cfg.DataDir, "store.boltdb")

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
		raft.ServerAddress(addr),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	)

	sm := fsm.NewRoutingFSM(cfg.Services)
	sm.Init(cfg.Serializer, standardLogger(cfg.Logger.Named("fsm")))

	memberlistConfig := memberlist.DefaultWANConfig()
	memberlistConfig.BindPort = cfg.DiscoveryPort
	memberlistConfig.Name = fmt.Sprintf("%s:%d", nodeID, cfg.RaftPort)
	memberlistConfig.Logger = standardLogger(cfg.Logger.Named("memberlist"))

	hasExistingState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return nil, err
	}

	raftServer, err := raft.NewRaft(raftConf, sm, logStore, stableStore, snapshotStore, grpcTransport.Transport())
	if err != nil {
		return nil, err
	}

	cfg.DiscoveryMethod.SetLogger(standardLogger(cfg.Logger.Named("discovery")))

	node := &Node{
		ID:               nodeID,
		address:          addr,
		dataDir:          cfg.DataDir,
		Raft:             raftServer,
		RaftPort:         cfg.RaftPort,
		TransportManager: grpcTransport,
		Serializer:       cfg.Serializer,
		DiscoveryPort:    cfg.DiscoveryPort,
		DiscoveryMethod:  cfg.DiscoveryMethod,
		memberlistConfig: memberlistConfig,
		logger:           cfg.Logger.Named("node"),
		snapshotEnabled:  cfg.SnapshotEnabled,
		hasExistingState: hasExistingState,
		bootstrap:        cfg.Bootstrap,
		formationTimeout: cfg.FormationTimeout,
	}

	return node, nil
}

func (n *Node) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return errors.New("node already started")
	}
	n.started = true
	n.stopping.Store(false)
	n.mu.Unlock()

	n.logger.Info("starting node")

	grpcListen, err := net.Listen("tcp", n.address)
	if err != nil {
		n.markStopped()
		return err
	}

	grpcServer := grpc.NewServer()
	n.GrpcServer = grpcServer

	n.TransportManager.Register(grpcServer)

	clientGrpcServer := NewClientGrpcService(n)
	ergrpc.RegisterRaftServer(grpcServer, clientGrpcServer)

	go func() {
		if err := grpcServer.Serve(grpcListen); err != nil {
			if !n.stopping.Load() {
				n.logger.Error("raft server stopped with error", "error", err)
			}
		}
	}()

	discoveryChan, err := n.DiscoveryMethod.Start(n.ID, n.RaftPort)
	if err != nil {
		grpcServer.Stop()
		n.GrpcServer = nil
		n.markStopped()
		return err
	}

	bufferedPeers := make([]string, 0)
	shouldBootstrap := false
	if !n.hasExistingState {
		shouldBootstrap = n.bootstrap
		if !shouldBootstrap {
			bufferedPeers, shouldBootstrap, err = n.evaluateBootstrap(ctx, discoveryChan)
			if err != nil {
				_ = n.DiscoveryMethod.Stop()
				grpcServer.Stop()
				n.GrpcServer = nil
				n.markStopped()
				return err
			}
		}
	}

	if shouldBootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(n.ID),
					Address: n.TransportManager.Transport().LocalAddr(),
				},
			},
		}

		if err := n.Raft.BootstrapCluster(configuration).Error(); err != nil {
			_ = n.DiscoveryMethod.Stop()
			grpcServer.Stop()
			n.GrpcServer = nil
			n.markStopped()
			return err
		}
	}

	n.memberlistConfig.Events = n
	list, err := memberlist.Create(n.memberlistConfig)
	if err != nil {
		_ = n.DiscoveryMethod.Stop()
		grpcServer.Stop()
		n.GrpcServer = nil
		n.markStopped()
		return err
	}
	n.memberlist = list

	go n.handleDiscoveredNodes(n.replayDiscoveredPeers(bufferedPeers, discoveryChan))

	n.hasExistingState = true

	n.logger.Info("node started", "raft_port", n.RaftPort, "discovery_port", n.DiscoveryPort)

	return nil
}

// Stop stops the node gracefully.
func (n *Node) Stop(ctx context.Context) (shutdownErr error) {
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return nil
	}
	n.started = false
	n.stopping.Store(true)
	list := n.memberlist
	raftNode := n.Raft
	grpcServer := n.GrpcServer
	discoveryMethod := n.DiscoveryMethod
	snapshotEnabled := n.snapshotEnabled
	n.mu.Unlock()

	n.logger.Info("stopping node")

	if snapshotEnabled && raftNode != nil {
		n.logger.Info("creating snapshot")

		err := runWithContext(ctx, func() error {
			return raftNode.Snapshot().Error()
		})
		if err != nil {
			n.logger.Error("failed to create snapshot", "error", err)
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}

	if discoveryMethod != nil {
		err := runWithContext(ctx, discoveryMethod.Stop)
		if err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		}
	}

	if list != nil {
		err := runWithContext(ctx, func() error {
			return list.Leave(10 * time.Second)
		})
		if err != nil {
			n.logger.Error("failed to leave from discovery", "error", err)
			shutdownErr = errors.Join(shutdownErr, err)
		}

		err = runWithContext(ctx, list.Shutdown)
		if err != nil {
			n.logger.Error("failed to shutdown discovery", "error", err)
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			n.logger.Info("discovery stopped")
		}
	}

	if raftNode != nil {
		err := runWithContext(ctx, func() error {
			return raftNode.Shutdown().Error()
		})
		if err != nil {
			n.logger.Error("failed to shutdown raft", "error", err)
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			n.logger.Info("raft stopped")
		}
	}

	if grpcServer != nil {
		err := gracefulStopGRPC(ctx, grpcServer)
		if err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			n.logger.Info("raft server stopped")
		}
	}

	n.mu.Lock()
	n.memberlist = nil
	n.GrpcServer = nil
	n.mu.Unlock()

	n.logger.Info("node stopped")

	if ctxErr := ctx.Err(); ctxErr != nil {
		shutdownErr = errors.Join(shutdownErr, ctxErr)
	}

	return shutdownErr
}

func (n *Node) markStopped() {
	n.mu.Lock()
	n.started = false
	n.stopping.Store(false)
	n.mu.Unlock()
}

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

			peerDetails, err := GetPeerDetails(peer)
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

	nodeID := uid.New(20)
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

func runWithContext(ctx context.Context, fn func() error) error {
	resultCh := make(chan error, 1)
	go func() {
		resultCh <- fn()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-resultCh:
		return err
	}
}

func gracefulStopGRPC(ctx context.Context, server *grpc.Server) error {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	select {
	case <-ctx.Done():
		server.Stop()
		<-done
		return ctx.Err()
	case <-done:
		return nil
	}
}

// handleDiscoveredNodes handles the discovered Node additions
func (n *Node) handleDiscoveredNodes(discoveryChan <-chan string) {
	for peer := range discoveryChan {
		details, err := GetPeerDetails(peer)
		if err != nil {
			continue
		}

		serverID := details.ServerID
		needToAddNode := true
		for _, server := range n.Raft.GetConfiguration().Configuration().Servers {
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
	result := n.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(nodeAddr), 0, 0)

	if result.Error() != nil {
		n.logger.Error("failed to add voter", "node_id", nodeID, "address", nodeAddr, "error", result.Error())
	}
}

// NotifyLeave triggered when a Node becomes unavailable after a period of time
// it will remove the unavailable Node from the Raft cluster
func (n *Node) NotifyLeave(node *memberlist.Node) {
	if !n.DiscoveryMethod.SupportsNodeAutoRemoval() {
		return
	}

	if !n.isLeader() {
		return
	}

	nodeID := strings.Split(node.Name, ":")[0]
	result := n.Raft.RemoveServer(raft.ServerID(nodeID), 0, 0)

	err := result.Error()
	if err != nil {
		n.logger.Error("failed to remove raft node", "node_id", nodeID, "error", err)
	}
}

func (n *Node) NotifyUpdate(_ *memberlist.Node) {
}

// RaftApply is used to apply any new logs to the raft cluster
// this method does automatic forwarding to Leader Node
func (n *Node) RaftApply(request any, timeout time.Duration) (any, error) {
	payload, err := n.Serializer.Serialize(request)
	if err != nil {
		return nil, err
	}

	if n.isLeader() {
		result := n.Raft.Apply(payload, timeout)
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

	response, err := ApplyOnLeader(n, payload)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (n *Node) isLeader() bool {
	if err := n.Raft.VerifyLeader().Error(); err != nil {
		return false
	}

	return true
}
