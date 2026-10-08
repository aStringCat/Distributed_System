package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"system/internal/cluster"
)

func main() {
	id := flag.Int("id", 0, "member ID, index into -peers")
	peers := flag.String("peers", "127.0.0.1:9000", "ordered comma-separated RPC addresses")
	httpAddr := flag.String("http", "127.0.0.1:8080", "HTTP listen address")
	data := flag.String("data", "data/node-0", "exclusive node data directory")
	startup := flag.Duration("startup-timeout", 15*time.Second, "deadline for initial peer connections")
	snapshotEvery := flag.Int("snapshot-every", 0, "snapshot after this many applied entries (0 disables)")
	flag.Parse()
	addresses := strings.Split(*peers, ",")
	for i := range addresses {
		addresses[i] = strings.TrimSpace(addresses[i])
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := cluster.Run(ctx, cluster.Config{ID: *id, Peers: addresses, HTTPAddr: *httpAddr, DataDir: *data, StartupTimeout: *startup, SnapshotEvery: *snapshotEvery})
	stop()
	if err != nil {
		log.Fatal(err)
	}
}
