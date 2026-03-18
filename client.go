package easyraft

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ergrpc "github.com/netrusov/easyraft/internal/grpc"
)

func applyOnLeader(node *Node, payload []byte) (any, error) {
	if node.raft.Leader() == "" {
		return nil, errors.New("unknown leader")
	}

	conn, err := grpc.NewClient(
		string(node.raft.Leader()),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.EmptyDialOption{},
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := ergrpc.NewRaftClient(conn)

	response, err := client.ApplyLog(context.Background(), &ergrpc.ApplyRequest{Request: payload})
	if err != nil {
		return nil, err
	}

	result, err := node.serializer.Deserialize(response.Response)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func getPeerDetails(address string) (*ergrpc.GetDetailsResponse, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.EmptyDialOption{},
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := ergrpc.NewRaftClient(conn)

	response, err := client.GetDetails(context.Background(), &ergrpc.GetDetailsRequest{})
	if err != nil {
		return nil, err
	}

	return response, nil
}
