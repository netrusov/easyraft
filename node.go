package easyraft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
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
	logger           *log.Logger
	snapshotEnabled  bool

	mu       sync.Mutex
	started  bool
	stopping atomic.Bool
}

const (
	defaultRaftLogCacheSize = 512
	defaultRaftLogLevel     = "Info"
)

// NewNode returns an EasyRaft node
func NewNode(raftPort, discoveryPort int, dataDir string, services []fsm.FSMService, serializer serializer.Serializer, discoveryMethod discovery.DiscoveryMethod, snapshotEnabled bool, resolveAdvertiseAddr string) (*Node, error) {
	advertiseAddr := "0.0.0.0"

	// resolve which address will be used to announce to members
	if resolveAdvertiseAddr != "" {
		advertiseAddr = util.GetOutboundIP(resolveAdvertiseAddr).String()
	}

	// default raft config
	addr := net.JoinHostPort(advertiseAddr, strconv.Itoa(raftPort))
	nodeID := uid.New(50)

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(nodeID)
	raftLogCacheSize := defaultRaftLogCacheSize
	raftConf.LogLevel = defaultRaftLogLevel

	// stable/log/snapshot store config
	if !util.IsDir(dataDir) {
		if err := util.RemoveCreateDir(dataDir); err != nil {
			return nil, err
		}
	}

	stableStoreFile := filepath.Join(dataDir, "store.boltdb")
	if util.FileExists(stableStoreFile) {
		if err := os.Remove(stableStoreFile); err != nil {
			return nil, err
		}
	}

	stableStore, err := raftboltdb.NewBoltStore(stableStoreFile)
	if err != nil {
		return nil, err
	}

	logStore, err := raft.NewLogCache(raftLogCacheSize, stableStore)
	if err != nil {
		return nil, err
	}

	var snapshotStore raft.SnapshotStore
	if !snapshotEnabled {
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

	sm := fsm.NewRoutingFSM(services)
	sm.Init(serializer)

	memberlistConfig := memberlist.DefaultWANConfig()
	memberlistConfig.BindPort = discoveryPort
	memberlistConfig.Name = fmt.Sprintf("%s:%d", nodeID, raftPort)

	raftServer, err := raft.NewRaft(raftConf, sm, logStore, stableStore, snapshotStore, grpcTransport.Transport())
	if err != nil {
		return nil, err
	}

	logger := log.Default()
	logger.SetPrefix("[EasyRaft] ")

	node := &Node{
		ID:               nodeID,
		address:          addr,
		dataDir:          dataDir,
		Raft:             raftServer,
		RaftPort:         raftPort,
		TransportManager: grpcTransport,
		Serializer:       serializer,
		DiscoveryPort:    discoveryPort,
		DiscoveryMethod:  discoveryMethod,
		memberlistConfig: memberlistConfig,
		logger:           logger,
		snapshotEnabled:  snapshotEnabled,
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

	n.logger.Println("Starting Node...")

	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(n.ID),
				Address: n.TransportManager.Transport().LocalAddr(),
			},
		},
	}

	if err := n.Raft.BootstrapCluster(configuration).Error(); err != nil {
		n.markStopped()
		return err
	}

	n.memberlistConfig.Events = n
	list, err := memberlist.Create(n.memberlistConfig)
	if err != nil {
		n.markStopped()
		return err
	}
	n.memberlist = list

	grpcListen, err := net.Listen("tcp", n.address)
	if err != nil {
		_ = list.Shutdown()
		n.memberlist = nil
		n.markStopped()
		return err
	}

	grpcServer := grpc.NewServer()
	n.GrpcServer = grpcServer

	n.TransportManager.Register(grpcServer)

	clientGrpcServer := NewClientGrpcService(n)
	ergrpc.RegisterRaftServer(grpcServer, clientGrpcServer)

	discoveryChan, err := n.DiscoveryMethod.Start(n.ID, n.RaftPort)
	if err != nil {
		_ = grpcListen.Close()
		_ = list.Shutdown()
		n.memberlist = nil
		n.GrpcServer = nil
		n.markStopped()
		return err
	}
	go n.handleDiscoveredNodes(discoveryChan)

	go func() {
		if err := grpcServer.Serve(grpcListen); err != nil {
			if !n.stopping.Load() {
				n.logger.Printf("Raft server stopped with error: %q\n", err.Error())
			}
		}
	}()

	n.logger.Printf("Node started on port %d and discovery port %d\n", n.RaftPort, n.DiscoveryPort)
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

	n.logger.Println("Stopping Node...")

	if snapshotEnabled && raftNode != nil {
		n.logger.Println("Creating snapshot...")

		err := runWithContext(ctx, func() error {
			return raftNode.Snapshot().Error()
		})
		if err != nil {
			n.logger.Println("Failed to create snapshot!")
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
			n.logger.Printf("Failed to leave from discovery: %q\n", err.Error())
			shutdownErr = errors.Join(shutdownErr, err)
		}

		err = runWithContext(ctx, list.Shutdown)
		if err != nil {
			n.logger.Printf("Failed to shutdown discovery: %q\n", err.Error())
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			n.logger.Println("Discovery stopped")
		}
	}

	if raftNode != nil {
		err := runWithContext(ctx, func() error {
			return raftNode.Shutdown().Error()
		})
		if err != nil {
			n.logger.Printf("Failed to shutdown Raft: %q\n", err.Error())
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			n.logger.Println("Raft stopped")
		}
	}

	if grpcServer != nil {
		err := gracefulStopGRPC(ctx, grpcServer)
		if err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
		} else {
			n.logger.Println("Raft Server stopped")
		}
	}

	n.mu.Lock()
	n.memberlist = nil
	n.GrpcServer = nil
	n.mu.Unlock()

	n.logger.Println("Node Stopped!")

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
		detailsResp, err := GetPeerDetails(peer)
		if err != nil {
			continue
		}

		serverID := detailsResp.ServerId
		needToAddNode := true
		for _, server := range n.Raft.GetConfiguration().Configuration().Servers {
			if server.ID == raft.ServerID(serverID) || string(server.Address) == peer {
				needToAddNode = false
				break
			}
		}

		if needToAddNode && n.memberlist != nil {
			peerHost := strings.Split(peer, ":")[0]
			peerDiscoveryAddr := fmt.Sprintf("%s:%d", peerHost, detailsResp.DiscoveryPort)
			if _, err = n.memberlist.Join([]string{peerDiscoveryAddr}); err != nil {
				log.Printf("failed to join to cluster using discovery address: %s\n", peerDiscoveryAddr)
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
		log.Println(result.Error().Error())
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
		n.logger.Printf("Failed to remove Raft node: %q\n", err.Error())
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
