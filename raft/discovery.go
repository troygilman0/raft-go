package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
	"time"
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

func (svc *DiscoveryService) Start(port string) error {
	slog.Info("Starting discovery service")
	rpcServer, err := newRPCServer(svc)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/rpc", rpcServer)
	mux.HandleFunc("/command", svc.handleCommand)
	if err := http.ListenAndServe(":"+port, mux); err != http.ErrServerClosed {
		return err
	}
	return nil
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
		slog.Info("handleCommand()")

		// decode input
		var input CommandInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			return err
		}

		// find a possible leader
		if svc.leader == "" {
			svc.leader = svc.getRandomServer()
			if svc.leader == "" {
				return errors.New("no servers have been registered")
			}
		}

		args := &CommandArgs{
			Command: input.Command,
		}
		result := &CommandResult{}

		// make rpc calls until we find the leader and apply the command
		for {
			client, err := rpc.DialHTTPPath("tcp", svc.leader, "/rpc")
			if err != nil {
				svc.leader = svc.getRandomServer()
				continue
			}

			{ // make call with cliennt
				timeout := time.NewTimer(rpcTimeoutDuration)
				call := client.Go("RPCGateway.CommandRPC", args, result, nil)
				select {
				case <-call.Done:
					err = call.Error
				case <-timeout.C:
					err = errors.New("rpc timed out")
				}
				if err != nil {
					client.Close()
					return fmt.Errorf("calling %s - %s", svc.leader, err.Error())
				}
			}

			if result.Success {
				break
			} else {
				if result.Redirect != "" {
					svc.leader = result.Redirect
				} else {
					svc.leader = svc.getRandomServer()
				}
			}
		}
		return nil
	}()

	if err != nil {
		slog.Error("handleCommand()", "error", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (svc *DiscoveryService) getRandomServer() string {
	svc.mutex.Lock()
	defer svc.mutex.Unlock()
	k := rand.Intn(len(svc.servers))
	for server := range svc.servers {
		if k == 0 {
			return server
		}
		k--
	}
	return ""
}

type CommandInput struct {
	Command string `json:"command"`
}
