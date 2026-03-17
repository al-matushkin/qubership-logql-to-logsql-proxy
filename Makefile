BINARY     := logql-proxy
CMD        := ./cmd/logql-proxy
IMAGE_NAME := logql-proxy
IMAGE_TAG  := latest

.PHONY: build test lint docker-build run clean

## build: compile the binary into ./bin/logql-proxy
build:
	mkdir -p bin
	go build -ldflags="-s -w" -o bin/$(BINARY) $(CMD)

## test: run all unit tests with race detector
test:
	go test -race -count=1 ./...

## lint: run golangci-lint (requires golangci-lint to be installed)
lint:
	golangci-lint run ./...

## docker-build: build the Docker image
docker-build:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

## run: start the proxy locally (reads config from config.yaml or env vars)
run: build
	./bin/$(BINARY)

## clean: remove build artifacts
clean:
	rm -rf bin/
