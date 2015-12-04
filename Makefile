all: server replayer

server:
	go build -o ./bin/tidb-server ./cmd/server 

replayer:
	go build -o ./bin/tidb-replayer ./cmd/replayer

test:
	go test --race ./relay/...