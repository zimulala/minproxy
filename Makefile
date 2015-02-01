all: build

build:
	go build -o bin/proxy ./cmd/cmd

clean:
	@rm -rf bin

test:
	go test ./...
