.PHONY: build test up down collector load bench demo demo-deploy

# The live demo, furcate.sanidhya.work: a snapshot of the runs in demo/runs.txt
# (and their branches) from the local stores, served as static files by Vercel.
DEMO_NOTE := Real runs of Claude Opus 5.5 on the csv-report example, recorded on 10 Nov 2026 on Docker and on tine, with their forks and eval cases.
VERCEL := npx -y vercel@65.3.1 --scope sanidhyasins-projects

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

demo: build
	rm -rf demo/site
	./bin/furcate snapshot -o demo/site -note "$(DEMO_NOTE)" $$(cat demo/runs.txt)
	cp demo/vercel.json demo/site/

demo-deploy: demo
	cd demo/site && $(VERCEL) link --yes --project furcate && $(VERCEL) deploy --prod --archive=tgz
