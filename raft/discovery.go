package raft

import (
	"container/heap"
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type DiscoveryService struct {
	servers       map[string]struct{}
	mutex         sync.Mutex
	commands      *commandQueue
	commandsMutex *sync.Mutex
	clients       *sync.Map
	logger        *slog.Logger
}

func NewDiscoveryService(logger *slog.Logger) *DiscoveryService {
	commands := &commandQueue{}
	heap.Init(commands)
	return &DiscoveryService{
		servers:       make(map[string]struct{}),
		commands:      commands,
		commandsMutex: &sync.Mutex{},
		clients:       &sync.Map{},
		logger:        logger,
	}
}

func (svc *DiscoveryService) Start(port string) error {
	slog.Info("Starting discovery service")
	go svc.startHandler()
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

func (svc *DiscoveryService) startHandler() {
	leader := ""
	for {
		msg, ok := func() (commandQueueMsg, bool) {
			svc.commandsMutex.Lock()
			defer svc.commandsMutex.Unlock()
			if svc.commands.Len() == 0 {
				return commandQueueMsg{}, false
			}
			return heap.Pop(svc.commands).(commandQueueMsg), true
		}()

		if ok {
			if msg.leader == "" {
				if leader == "" || msg.random {
					leader = svc.getRandomServer()
				}
				msg.leader = leader
			} else {
				leader = msg.leader
			}

			go func(commandQueueMsg) {
				args := &CommandArgs{
					Command: msg.command,
				}
				result := &CommandResult{}

				// make rpc calls until we find the leader and apply the command
				if err := sendRPC(svc.clients, "RPCGateway.CommandRPC", msg.leader, args, result); err != nil {
					if svc.logger != nil {
						svc.logger.Error("calling %s - %s\n", msg.leader, err.Error())
					}
				}

				if result.Success {
					msg.done <- struct{}{}
				} else {
					if result.Redirect != "" {
						msg.leader = result.Redirect
					} else {
						msg.leader = ""
						msg.random = true
					}
					svc.commandsMutex.Lock()
					heap.Push(svc.commands, msg)
					svc.commandsMutex.Unlock()
				}
			}(msg)
		}
		time.Sleep(1 * time.Millisecond)

	}
}

func (svc *DiscoveryService) handleCommand(w http.ResponseWriter, r *http.Request) {
	err := func() error {
		// decode input
		var input CommandInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			return err
		}

		if svc.logger != nil {
			svc.logger.Info("Discovery handleCommand()", "command", input.Command)
		}

		svc.commandsMutex.Lock()
		msg := commandQueueMsg{
			time:    time.Now(),
			command: input.Command,
			done:    make(chan struct{}),
		}
		heap.Push(svc.commands, msg)
		svc.commandsMutex.Unlock()

		<-msg.done
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

type commandQueueMsg struct {
	time    time.Time
	command string
	leader  string
	random  bool
	done    chan struct{}
}

type commandQueue []commandQueueMsg

func (q commandQueue) Len() int           { return len(q) }
func (q commandQueue) Less(i, j int) bool { return q[i].time.Before(q[j].time) }
func (q commandQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }

func (q *commandQueue) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*q = append(*q, x.(commandQueueMsg))
}

func (q *commandQueue) Pop() any {
	old := *q
	n := len(old)
	x := old[n-1]
	*q = old[0 : n-1]
	return x
}
