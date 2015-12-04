all: recorder replayer

recorder:
	go build -o ./bin/tidb-recorder ./cmd/recorder 

replayer:
	go build -o ./bin/tidb-replayer ./cmd/replayer

test:
	go test --race ./relay/...