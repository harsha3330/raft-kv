package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	httpd "github.com/harsha3330/raft-kv/http"
)

var (
	logPath string
	node    httpd.Node
	peers   string
)

func init() {
	flag.BoolVar(&node.IsLeader, "leader", false, "If the current is the cluster leader")
	flag.StringVar(&logPath, "log-path", "", "path to WAL file")
	flag.StringVar(&node.Id, "node-id", "node0", "node identifier")
	flag.StringVar(&node.Addr, "addr", ":8000", "http listen address")
	flag.StringVar(&peers, "peers", "", "list of cluster members")
}

func main() {
	flag.Parse()
	node.Peers = strings.Split(peers, ",")
	if logPath == "" {
		logPath = fmt.Sprintf("%s-wal.log", node.Id)
	}
	srv, err := httpd.NewServer(node, logPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(srv.Start())
}
