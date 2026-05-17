package main

import (
	"flag"
	"log"

	httpd "github.com/harsha3330/raft-kv/http"
)

var (
	logPath string
	nodeID  string
	addr    string
)

func init() {
	flag.StringVar(&logPath, "log-path", "wal.log", "path to WAL file")
	flag.StringVar(&nodeID, "node-id", "node0", "node identifier")
	flag.StringVar(&addr, "addr", ":8000", "http listen address")
}

func main() {
	flag.Parse()

	srv, err := httpd.NewServer(addr, logPath, nodeID)
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(srv.Start())
}
