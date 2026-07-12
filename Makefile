.PHONY: all tidy swagger build build-linux test clean run

all: tidy swagger build

tidy:
	go mod tidy

swagger:
	swag init -g cmd/livy-next/main.go

build: swagger
	mkdir -p bin
	go build -ldflags="-s -w" -o bin/livy-next cmd/livy-next/main.go

build-linux: swagger
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o bin/livy-next cmd/livy-next/main.go

test:
	go test -v ./...

clean:
	rm -rf bin docs/docs.go docs/swagger.json docs/swagger.yaml

run: build
	./bin/livy-next --addr :8999 --spark-remote sc://localhost:15002
