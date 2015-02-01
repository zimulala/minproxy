all: build

build:
	@tree -L
	go build -o bin/proxy ./cmd

clean:
	@rm -rf bin
	go clean -i ./...

test:
	go test ./...
