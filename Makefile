.PHONY: setup test test-race vet fmt lint build down

setup:
	@if [ ! -f go.work ]; then \
		echo "Initializing Go workspace..."; \
		go work init . ./tests; \
		echo "Workspace initialized."; \
	else \
		echo "Go workspace already initialized."; \
	fi

test:
	docker compose run --rm test

test-race:
	go test -race ./...
	cd tests && go test -race ./...

lint:
	golangci-lint run ./...
	cd tests && golangci-lint run ./...

vet:
	go vet ./...
	cd tests && go vet ./...

fmt:
	go fmt ./...
	cd tests && go fmt ./...

build:
	go build ./...
	cd tests && go build ./...

down:
	rm -f coverage.out tests/coverage.out
	docker compose down --remove-orphans
