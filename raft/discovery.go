package raft

import (
	"log"
	"sync"
)

type DiscoveryService struct {
	servers map[string]struct{}
	mutex   *sync.Mutex
}

func NewDiscoveryService() *DiscoveryService {
	return &DiscoveryService{
		servers: make(map[string]struct{}),
		mutex:   &sync.Mutex{},
	}
}

func (svc *DiscoveryService) Start(port string) {
	log.Println("Starting Discovery Service...")
	if err := serveRPC(svc, port); err != nil {
		log.Fatal(err)
	}
}

func (svc *DiscoveryService) DiscoverRPC(args *DiscoverArgs, result *DiscoverResult) error {
	// log.Println("DiscoverRPC", args)
	svc.mutex.Lock()
	defer svc.mutex.Unlock()
	svc.servers[args.Id] = struct{}{}
	result.Servers = make([]string, 0, len(svc.servers)-1)
	for server := range svc.servers {
		if server != args.Id {
			result.Servers = append(result.Servers, server)
		}
	}
	return nil
}

type DiscoverArgs struct {
	Id string
}

type DiscoverResult struct {
	Servers []string
}
