# Makefile for llmgateway. On this locked-down Windows box Go is portable and
# not on PATH; prefer make.ps1 there. On a normal *nix/dev box these targets work
# once `go` is on PATH.

BINARY := gateway
PKG    := ./cmd/gateway

export CGO_ENABLED := 0

.PHONY: build test vet run serve bench docker clean

build: ## Compile the static binary into ./bin
	go build -ldflags="-s -w" -o bin/$(BINARY) $(PKG)

test: ## Run all tests (no -race: this env has no C compiler)
	go test -timeout 60s ./...

vet: ## Static analysis
	go vet ./...

run: ## CLI demo: serial vs pooled
	go run $(PKG) embed -n 2000 -workers 32

serve: ## Start the HTTP API on :8080 (mock provider)
	go run $(PKG) serve -addr :8080 -workers 16 -cache-size 1024

bench: ## Pool speedup benchmark
	go test -bench BenchmarkPoolSpeedup -benchtime 3x ./internal/embed

docker: ## Build the container image
	docker build -t llmgateway:latest .

clean: ## Remove build artifacts
	rm -rf bin
