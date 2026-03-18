grpc:
	protoc --go_out=./internal --go-grpc_out=./internal internal/proto/*.proto

download:
	go mod tidy

install: download grpc
