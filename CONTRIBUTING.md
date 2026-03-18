# Contributing

## Protobuf Workflow

When changing the gRPC API, update the source schema first:

1. Edit
   [proto/raft.proto](/home/netrusov/projects/github.com/netrusov/easyraft/proto/raft.proto).
2. Regenerate the Go bindings:
    ```bash
    make grpc
    ```

Rules:

- Do not hand-edit generated protobuf output unless you are explicitly fixing
  the generator workflow itself.
- Commit both the `.proto` change and the regenerated Go file together.
- If you add or rename RPC fields, update the runtime call sites that use those
  types in:
  - [client.go](/home/netrusov/projects/github.com/netrusov/easyraft/client.go)
  - [client_services.go](/home/netrusov/projects/github.com/netrusov/easyraft/client_services.go)

If `make grpc` fails because `protoc` or the Go plugins are missing, install
them first and rerun the command rather than editing generated files manually.
