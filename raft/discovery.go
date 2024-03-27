package raft

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/rpc"
	"sync"
)

type DiscoveryService struct {
	leader  string
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
	rpcServer, err := newRPCServer(svc)
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/rpc", rpcServer)
	mux.HandleFunc("/command", svc.handleCommand)
	log.Fatal(http.ListenAndServe(":"+port, mux))
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

func (svc *DiscoveryService) handleCommand(w http.ResponseWriter, r *http.Request) {
	err := func() error {
		log.Println("HandleCommand")

		// decode input
		var input CommandInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			return err
		}

		// find a possible leader
		if svc.leader == "" {
			for server := range svc.servers {
				svc.leader = server
				break
			}
			if svc.leader == "" {
				return errors.New("no servers have been registered")
			}
		}

		args := &CommandArgs{
			Command: input.Command,
		}
		result := &CommandResult{}

		// make rpc calls until we find the leader and apply the command
		addr := svc.leader
		for {
			client, err := rpc.DialHTTPPath("tcp", addr, "/rpc")
			if err != nil {
				return err
			}

			if err := client.Call("RPCGateway.CommandRPC", args, result); err != nil {
				return err
			}

			if result.Success {
				break
			} else {
				if result.Redirect != "" {
					addr = result.Redirect
				} else {
					return errors.New("command could not be applied")
				}
			}
		}
		return nil
	}()

	if err != nil {
		log.Println(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

type CommandInput struct {
	Command string `json:"command"`
}
