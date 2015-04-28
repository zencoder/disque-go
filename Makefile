GO ?= godep go
COVERAGEDIR = coverage

all: build test cover
godep:
	go get github.com/tools/godep
godep-save:
	godep save ./...
build:
	if [ ! -d bin ]; then mkdir bin; fi
	$(GO) build -v ./...
fmt:
	$(GO) fmt ./...
test:
	if [ ! -d coverage ]; then mkdir coverage; fi
	$(GO) test -v ./... -race -cover -coverprofile=$(COVERAGEDIR)/disque.coverprofile
cover:
	$(GO) tool cover -html=$(COVERAGEDIR)/disque.coverprofile -o $(COVERAGEDIR)/disque.html
tc: test cover
bench:
	$(GO) test ./... -bench .
clean:
	$(GO) clean
	rm -rf coverage/
