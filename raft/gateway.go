package raft

import (
	"errors"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

const (
	rpcTimeoutDuration = 200 * time.Millisecond
)

type Gateway interface {
	Discover(args *DiscoverArgs, result *DiscoverResult) error
	AppendEntries(id string, args *AppendEntriesArgs, result *AppendEntriesResult) error
	RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error
	AppendEntriesMsg() <-chan AppendEntriesMsg
	RequestVoteMsg() <-chan RequestVoteMsg
	CommandMsg() <-chan CommandMsg
	Start() error
	Close() error
}

func NewRPCGateway(port string, discoveryAddr string) Gateway {
	return &RPCGateway{
		httpServer:       &http.Server{Addr: ":" + port},
		discoveryAddr:    discoveryAddr,
		clients:          sync.Map{},
		appendEntriesMsg: make(chan AppendEntriesMsg),
		requestVoteMsg:   make(chan RequestVoteMsg),
		commandMsg:       make(chan CommandMsg),
	}
}

type RPCGateway struct {
	httpServer       *http.Server
	discoveryAddr    string
	clients          sync.Map
	appendEntriesMsg chan AppendEntriesMsg
	requestVoteMsg   chan RequestVoteMsg
	commandMsg       chan CommandMsg
}

func (gateway *RPCGateway) Start() error {
	rpcServer, err := newRPCServer(gateway)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/rpc", rpcServer)
	gateway.httpServer.Handler = mux
	if err := gateway.httpServer.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func newRPCServer(handler any) (*rpc.Server, error) {
	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(handler); err != nil {
		return nil, err
	}
	return rpcServer, nil
}

func (gateway *RPCGateway) sendRPC(name string, addr string, args any, result any) error {
	var client *rpc.Client
	{ // find or create a client
		clientAny, ok := gateway.clients.Load(addr)
		if ok {
			client, ok = clientAny.(*rpc.Client)
			if !ok {
				return errors.New("client cache returned something other than *rpc.Client")
			}
		} else {
			var err error
			client, err = rpc.DialHTTPPath("tcp", addr, "/rpc")
			if err != nil {
				return err
			}
			gateway.clients.Store(addr, client)
		}
	}

	var err error
	{ // make call with client
		timeout := time.NewTimer(rpcTimeoutDuration)
		call := client.Go(name, args, result, nil)
		select {
		case <-call.Done:
			err = call.Error
		case <-timeout.C:
			err = errors.New("rpc timed out")
		}
		if err != nil {
			client.Close()
			gateway.clients.Delete(addr)
		}
	}
	return err
}

func (gateway *RPCGateway) Discover(args *DiscoverArgs, result *DiscoverResult) error {
	return gateway.sendRPC("DiscoveryService.DiscoverRPC", gateway.discoveryAddr, args, result)
}

func (gateway *RPCGateway) AppendEntries(id string, args *AppendEntriesArgs, result *AppendEntriesResult) error {
	return gateway.sendRPC("RPCGateway.AppendEntriesRPC", id, args, result)
}

func (gateway *RPCGateway) RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error {
	return gateway.sendRPC("RPCGateway.RequestVoteRPC", id, args, result)
}

func (gateway *RPCGateway) AppendEntriesRPC(args *AppendEntriesArgs, result *AppendEntriesResult) error {
	done := make(chan struct{})
	gateway.appendEntriesMsg <- AppendEntriesMsg{
		args:   args,
		result: result,
		done:   done,
	}
	<-done
	return nil
}

func (gateway *RPCGateway) RequestVoteRPC(args *RequestVoteArgs, result *RequestVoteResult) error {
	done := make(chan struct{})
	gateway.requestVoteMsg <- RequestVoteMsg{
		args:   args,
		result: result,
		done:   done,
	}
	<-done
	return nil
}

func (gateway *RPCGateway) CommandRPC(args *CommandArgs, result *CommandResult) error {
	done := make(chan struct{})
	gateway.commandMsg <- CommandMsg{
		Args:   args,
		Result: result,
		Done:   done,
	}
	<-done
	return nil
}

func (gateway *RPCGateway) AppendEntriesMsg() <-chan AppendEntriesMsg {
	return gateway.appendEntriesMsg
}

func (gateway *RPCGateway) RequestVoteMsg() <-chan RequestVoteMsg {
	return gateway.requestVoteMsg
}

func (gateway *RPCGateway) CommandMsg() <-chan CommandMsg {
	return gateway.commandMsg
}

func (gateway *RPCGateway) Close() error {
	{ // close clients
		gateway.clients.Range(func(key, value any) bool {
			client, ok := value.(*rpc.Client)
			if ok {
				client.Close()
			}
			return true
		})
	}
	{ // close http server
		if err := gateway.httpServer.Close(); err != nil {
			return err
		}
	}
	{ // flush channels
	flushLoop:
		for {
			select {
			case msg := <-gateway.appendEntriesMsg:
				msg.done <- struct{}{}
			case msg := <-gateway.requestVoteMsg:
				msg.done <- struct{}{}
			case msg := <-gateway.commandMsg:
				msg.Done <- struct{}{}
			default:
				break flushLoop
			}
		}
	}
	return nil
}
