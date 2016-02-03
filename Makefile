export GO15VENDOREXPERIMENT = 1
COVERAGEDIR = coverage

all: build test cover
build:
	go build -v ./disque
fmt:
	go fmt ./disque
test:
	if [ ! -d coverage ]; then mkdir coverage; fi
	go test -v ./disque -race -cover -coverprofile=$(COVERAGEDIR)/disque.coverprofile
cover:
	go tool cover -html=$(COVERAGEDIR)/disque.coverprofile -o $(COVERAGEDIR)/disque.html
tc: test cover
bench:
	go test ./disque -bench .
clean:
	go clean
	rm -rf coverage/
