.PHONY: all tidy swagger build test clean run

all: tidy swagger build

tidy:
	go mod tidy

swagger:
	swag init -g cmd/livy-next/main.go

build:
	mkdir -p bin
	go build -ldflags="-s -w" -o bin/livy-next cmd/livy-next/main.go

test:
	go test -v ./...

clean:
	rm -rf bin docs

run: build
	./bin/livy-next --addr :8999 --spark-remote sc://localhost:15002
