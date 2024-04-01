package raft

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	numServers := 5
	discoveryPort := 8000
	discoveryAddr := "localhost:" + strconv.Itoa(discoveryPort)

	go NewDiscoveryService().Start(strconv.Itoa(discoveryPort))

	servers := make([]*Server, numServers)
	for i := range numServers {
		port := discoveryPort + i + 1
		portStr := strconv.Itoa(port)
		config := ServerConfig{
			Id:    "localhost:" + portStr,
			Debug: true,
		}
		servers[i] = NewServer(config)
		go servers[i].Start(NewRPCGateway(portStr, discoveryAddr))
	}

	for {
		i := rand.Intn(numServers)
		port := discoveryPort + i + 1
		portStr := strconv.Itoa(port)
		time.Sleep(time.Second)
		servers[i].Close()
		time.Sleep(time.Second)
		go servers[i].Start(NewRPCGateway(portStr, discoveryAddr))
	}
}
