all: server replayer

server:
	go build -o ./bin/tidb_server ./cmd/server 

replayer:
	go build -o ./bin/tidb_replayer ./cmd/replayer