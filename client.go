package easyraft

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	ergrpc "github.com/netrusov/easyraft/grpc"
)

func ApplyOnLeader(node *Node, payload []byte) (any, error) {
	if node.Raft.Leader() == "" {
		return nil, errors.New("unknown leader")
	}

	conn, err := grpc.NewClient(
		string(node.Raft.Leader()),
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

	result, err := node.Serializer.Deserialize(response.Response)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func GetPeerDetails(address string) (*ergrpc.GetDetailsResponse, error) {
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
