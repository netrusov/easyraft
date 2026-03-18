package easyraft

import (
	"context"

	ergrpc "github.com/netrusov/easyraft/internal/grpc"
)

type clientGRPCService struct {
	node *Node
	ergrpc.UnimplementedRaftServer
}

func newClientGRPCService(node *Node) *clientGRPCService {
	return &clientGRPCService{
		node: node,
	}
}

func (s *clientGRPCService) ApplyLog(ctx context.Context, request *ergrpc.ApplyRequest) (*ergrpc.ApplyResponse, error) {
	result := s.node.raft.Apply(request.GetRequest(), 0)
	if result.Error() != nil {
		return nil, result.Error()
	}

	respPayload, err := s.node.serializer.Serialize(result.Response())
	if err != nil {
		return nil, err
	}

	return &ergrpc.ApplyResponse{Response: respPayload}, nil
}

func (s *clientGRPCService) GetDetails(ctx context.Context, _ *ergrpc.GetDetailsRequest) (*ergrpc.GetDetailsResponse, error) {
	return &ergrpc.GetDetailsResponse{
		ServerID:         s.node.ID,
		DiscoveryPort:    int32(s.node.discoveryPort),
		HasExistingState: s.node.hasExistingState,
	}, nil
}
