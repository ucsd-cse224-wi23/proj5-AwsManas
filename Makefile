.PHONY: install
install:
	rm -rf bin
	GOBIN=$(PWD)/bin go install ./...

.PHONY: run-blockstore
run-blockstore:
	go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l

.PHONY: run-raft
run-raft:
	go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i $(IDX)

.PHONY: test
test:
	rm -rf ./test/_bin
	GOBIN="/Users/manas/Desktop/UCSD - Classes /Winter 23/CSE 224/proj5-AwsManas"/test/_bin go get github.com/mattn/go-sqlite3
	GOBIN="/Users/manas/Desktop/UCSD - Classes /Winter 23/CSE 224/proj5-AwsManas"/test/_bin go install ./...
	go test -v ./test/...

.PHONY: specific-test
specific-test:
	rm -rf ./test/_bin
	GOBIN="/Users/manas/Desktop/UCSD - Classes /Winter 23/CSE 224/proj5-AwsManas"/test/_bin go get github.com/mattn/go-sqlite3
	GOBIN="/Users/manas/Desktop/UCSD - Classes /Winter 23/CSE 224/proj5-AwsManas"/test/_bin go install ./...
	go test -v -run $(TEST_REGEX) -count=1 ./test/...

.PHONY: clean
clean:
	rm -rf bin/ test/_bin
