COVERAGEDIR = coverage

all: build test cover
build:
	go build -v ./disque
fmt:
	go fmt ./disque
test:
	go test -v ./disque -race -cover -coverprofile=disque.coverprofile
cover:
	go tool cover -html=$(COVERAGEDIR)/disque.coverprofile -o $(COVERAGEDIR)/disque.html
tc: test cover
bench:
	go test ./disque -bench .
clean:
	go clean
	rm -rf coverage/
