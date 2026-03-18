package easyraft

import (
	"context"

	ergrpc "github.com/netrusov/easyraft/grpc"
)

func NewClientGrpcService(node *Node) *ClientGrpcServices {
	return &ClientGrpcServices{
		Node: node,
	}
}

type ClientGrpcServices struct {
	Node *Node
	ergrpc.UnimplementedRaftServer
}

func (s *ClientGrpcServices) ApplyLog(ctx context.Context, request *ergrpc.ApplyRequest) (*ergrpc.ApplyResponse, error) {
	result := s.Node.Raft.Apply(request.GetRequest(), 0)
	if result.Error() != nil {
		return nil, result.Error()
	}

	respPayload, err := s.Node.Serializer.Serialize(result.Response())
	if err != nil {
		return nil, err
	}

	return &ergrpc.ApplyResponse{Response: respPayload}, nil
}

func (s *ClientGrpcServices) GetDetails(ctx context.Context, _ *ergrpc.GetDetailsRequest) (*ergrpc.GetDetailsResponse, error) {
	return &ergrpc.GetDetailsResponse{
		ServerID:         s.Node.ID,
		DiscoveryPort:    int32(s.Node.DiscoveryPort),
		HasExistingState: s.Node.hasExistingState,
	}, nil
}
