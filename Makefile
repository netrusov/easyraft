grpc:
	protoc --go_out=. --go-grpc_out=. proto/*.proto

download:
	go mod tidy

install: download grpc
