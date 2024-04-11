package main

import (
	"log"
	"log/slog"
	"os"

	"github.com/troygilman0/raft-go/raft"
)

const (
	port              = "8000"
	discoveryHostname = "raft-go_discovery_1"
)

func main() {
	hostname, ok := os.LookupEnv("HOSTNAME")
	if !ok {
		log.Fatal("could not find HOSTNAME env var")
	}

	_, discovery := os.LookupEnv("DISCOVERY")

	if discovery {
		raft.NewDiscoveryService(nil).Start(port)
	} else {
		config := raft.ServerConfig{
			Id:     hostname + ":" + port,
			Logger: slog.Default(),
		}
		raft.NewServer(config).Start(raft.NewRPCGateway(port, discoveryHostname+":"+port))
	}
}
