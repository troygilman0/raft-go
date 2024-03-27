package raft

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/rpc"
	"sync"
)

type Gateway interface {
	Discover(args *DiscoverArgs, result *DiscoverResult) error
	AppendEntries(id string, args *AppendEntriesArgs, result *AppendEntriesResult) error
	RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error
	AppendEntriesMsg() <-chan *AppendEntriesMsg
	RequestVoteMsg() <-chan *RequestVoteMsg
}

func NewRPCGateway(port string, discoveryAddr string) Gateway {
	gateway := &RPCGateway{
		port:             port,
		discoveryAddr:    discoveryAddr,
		clients:          sync.Map{},
		appendEntriesMsg: make(chan *AppendEntriesMsg),
		requestVoteMsg:   make(chan *RequestVoteMsg),
	}
	go func() {
		rpcServer, err := newRPCServer(gateway)
		if err != nil {
			log.Fatal(err)
		}
		mux := http.NewServeMux()
		mux.Handle("/", rpcServer)
		mux.HandleFunc("POST /command", gateway.handleCommand)
		log.Fatal(http.ListenAndServe(":"+port, mux))
	}()
	return gateway
}

type RPCGateway struct {
	port             string
	discoveryAddr    string
	clients          sync.Map
	appendEntriesMsg chan *AppendEntriesMsg
	requestVoteMsg   chan *RequestVoteMsg
}

func newRPCServer(handler any) (*rpc.Server, error) {
	rpcServer := rpc.NewServer()
	if err := rpcServer.Register(handler); err != nil {
		return nil, err
	}
	return rpcServer, nil
}

func (gateway *RPCGateway) sendRPC(name string, addr string, args any, result any) error {
	clientAny, ok := gateway.clients.Load(addr)
	var client *rpc.Client
	if ok {
		client, ok = clientAny.(*rpc.Client)
		if !ok {
			return errors.New("client cache returned something other than *rpc.Client")
		}
	} else {
		var err error
		client, err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			return err
		}
		gateway.clients.Store(addr, client)
	}
	if err := client.Call(name, args, result); err != nil {
		if err := client.Close(); err != nil {
			log.Println(err)
		}
		gateway.clients.Delete(addr)
		return err
	}
	return nil
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
	gateway.appendEntriesMsg <- &AppendEntriesMsg{
		args:   args,
		result: result,
		done:   done,
	}
	<-done
	return nil
}

func (gateway *RPCGateway) RequestVoteRPC(args *RequestVoteArgs, result *RequestVoteResult) error {
	done := make(chan struct{})
	gateway.requestVoteMsg <- &RequestVoteMsg{
		args:   args,
		result: result,
		done:   done,
	}
	<-done
	return nil
}

func (gateway *RPCGateway) AppendEntriesMsg() <-chan *AppendEntriesMsg {
	return gateway.appendEntriesMsg
}

func (gateway *RPCGateway) RequestVoteMsg() <-chan *RequestVoteMsg {
	return gateway.requestVoteMsg
}

func (gateway *RPCGateway) handleCommand(w http.ResponseWriter, r *http.Request) {
	var input CommandInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		log.Fatal(err)
	}

}

type CommandInput struct {
	Command string
}
