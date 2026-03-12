.ONESHELL:
SHELL = /bin/bash
.SHELLFLAGS = -euo pipefail -c

.PHONY: test test-integration test-all build

build:
	go build -o firestore2csv .

test:
	go test -v ./...

test-integration:
	@echo "Starting Firestore emulator..."
	firebase emulators:start --only firestore --project test-project &
	sleep 5
	export FIRESTORE_EMULATOR_HOST=localhost:8686
	go test -v -tags integration -count=1 ./... || EXIT_CODE=$$?
	kill %1 2>/dev/null || true
	exit $${EXIT_CODE:-0}

test-all: test test-integration
