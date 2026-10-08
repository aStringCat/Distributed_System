.PHONY: fmt build check

fmt:
	gofmt -w internal cmd

build:
	mkdir -p bin
	go build -o bin/raftkv ./cmd/raftkv

check:
	@test -z "$$(gofmt -l internal cmd)" || (gofmt -l internal cmd; exit 1)
	go vet ./...
	go build ./...
