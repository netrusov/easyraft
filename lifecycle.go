package easyraft

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/hashicorp/raft"
	ergrpc "github.com/netrusov/easyraft/internal/grpc"
	"google.golang.org/grpc"
)

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
	n.grpcServer = grpcServer

	n.transportManager.Register(grpcServer)

	clientGrpcServer := newClientGRPCService(n)
	ergrpc.RegisterRaftServer(grpcServer, clientGrpcServer)

	go func() {
		if err := grpcServer.Serve(grpcListen); err != nil {
			if !n.stopping.Load() {
				n.logger.Error("raft server stopped with error", "error", err)
			}
		}
	}()

	discoveryChan, err := n.discoveryMethod.Start(n.ID, n.raftPort)
	if err != nil {
		grpcServer.Stop()
		n.grpcServer = nil
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
				_ = n.discoveryMethod.Stop()
				grpcServer.Stop()
				n.grpcServer = nil
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
					Address: n.transportManager.Transport().LocalAddr(),
				},
			},
		}

		if err := n.raft.BootstrapCluster(configuration).Error(); err != nil {
			_ = n.discoveryMethod.Stop()
			grpcServer.Stop()
			n.grpcServer = nil
			n.markStopped()
			return err
		}
	}

	n.memberlistConfig.Events = n
	list, err := memberlist.Create(n.memberlistConfig)
	if err != nil {
		_ = n.discoveryMethod.Stop()
		grpcServer.Stop()
		n.grpcServer = nil
		n.markStopped()
		return err
	}
	n.memberlist = list

	go n.handleDiscoveredNodes(n.replayDiscoveredPeers(bufferedPeers, discoveryChan))

	n.hasExistingState = true

	n.logger.Info("node started", "raft_port", n.raftPort, "discovery_port", n.discoveryPort)

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
	raftNode := n.raft
	grpcServer := n.grpcServer
	discoveryMethod := n.discoveryMethod
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
	n.grpcServer = nil
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
