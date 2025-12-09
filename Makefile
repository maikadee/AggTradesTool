.PHONY: build run clean deps

# Binary name
BINARY=aggtrades

# Build the binary
build:
	go build -o $(BINARY) ./cmd/aggtrades

# Build optimized binary
release:
	go build -ldflags="-s -w" -o $(BINARY) ./cmd/aggtrades

# Run directly
run:
	go run ./cmd/aggtrades

# Run with arguments
run-eth:
	go run ./cmd/aggtrades --symbol ETHUSDT

# Clean build artifacts and temp files
clean:
	rm -f $(BINARY)
	rm -rf aggtrades_temp_*/
	rm -f *.parquet
	rm -f .checkpoint.json

# Download dependencies
deps:
	go mod tidy

# Format code
fmt:
	go fmt ./...

# Vet code
vet:
	go vet ./...
