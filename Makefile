.PHONY: build test up down collector load bench

build:
	go build -o bin/furcate ./cmd/furcate

test:
	go test -race ./...

up:
	docker compose up -d --wait

down:
	docker compose down

collector: build
	./bin/furcate collector

load: build
	./bin/furcate loadgen --duration 30s --concurrency 16

bench: build
	./bin/furcate bench --n 100
