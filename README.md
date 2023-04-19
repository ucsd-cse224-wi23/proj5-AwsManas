$ go run cmd/SurfstoreRaftServerExec/main.go -f configfile.txt -i 0 -b localhost:8080

Would start the server with a configuration file of configfile.txt. It would tell the server that it is server 0 in the configured servers. And the server that starts will assume that there is a BlockStore running on localhost:8080. 

The client now also takes the same configuration file so that it knows where the servers are running. You can run the client with:
$ go run cmd/SurfstoreClientExec/main.go -f configfile.txt baseDir blockSize

A Makefile is provided to things easier, for example to run a BlockStore you should type:
$ make run-blockstore

Then in a separate terminal you can run a raft server with:
$ make IDX=0 run-raft

RAFT based protocol 
