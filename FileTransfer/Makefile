# Source lives at the repo root; the deployable package (FT_HOME) is ./deploy.
PKG       := ./...
BUILD_OUT := build/filetransfer
DEPLOY    := deploy

.PHONY: build start-master stop-master start-worker stop-worker certs tidy vet test clean

# Build the binary, then copy it into the deploy package (deploy/lib).
build:
	@mkdir -p build $(DEPLOY)/lib
	go build -o $(BUILD_OUT) .
	cp $(BUILD_OUT) $(DEPLOY)/lib/filetransfer
	@echo "built $(BUILD_OUT) -> $(DEPLOY)/lib/filetransfer"

# Run via the deploy package's scripts (deploy/bin resolves FT_HOME=deploy).
start-master: build
	$(DEPLOY)/bin/start.sh master
stop-master:
	$(DEPLOY)/bin/stop.sh master
start-worker: build
	$(DEPLOY)/bin/start.sh worker
stop-worker:
	$(DEPLOY)/bin/stop.sh worker

# (Re)generate the self-signed CA + server + worker certs into deploy/certs.
certs:
	$(DEPLOY)/bin/gen-certs.sh

tidy:
	go mod tidy

vet:
	go vet $(PKG)

test:
	go test $(PKG)

clean:
	rm -rf build $(DEPLOY)/lib $(DEPLOY)/logs/*.pid
