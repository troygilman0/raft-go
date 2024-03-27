package raft

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
)

type Gateway interface {
	Discover(args *DiscoverArgs, result *DiscoverResult) error
	AppendEntries(id string, args *AppendEntriesArgs, result *AppendEntriesResult) error
	AppendEntriesMsg() <-chan *AppendEntriesMsg
	RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error
	RequestVoteMsg() <-chan *RequestVoteMsg
}

func NewRPCGateway(port string, discoveryAddr string) Gateway {
	gateway := &RPCGateway{
		port:             port,
		discoveryAddr:    discoveryAddr,
		clients:          make(map[string]*rpc.Client),
		appendEntriesMsg: make(chan *AppendEntriesMsg),
		requestVoteMsg:   make(chan *RequestVoteMsg),
	}
	go func() {
		if err := serveRPC(gateway, port); err != nil {
			log.Fatal(err)
		}
	}()
	return gateway
}

type RPCGateway struct {
	port             string
	discoveryAddr    string
	clients          map[string]*rpc.Client
	appendEntriesMsg chan *AppendEntriesMsg
	requestVoteMsg   chan *RequestVoteMsg
}

func serveRPC(handler any, port string) error {
	if err := rpc.Register(handler); err != nil {
		return err
	}
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	return http.Serve(listener, nil)
}

func (gateway *RPCGateway) sendRPC(name string, addr string, args any, result any) error {
	client, ok := gateway.clients[addr]
	if !ok {
		var err error
		client, err = rpc.DialHTTP("tcp", addr)
		if err != nil {
			return err
		}
		gateway.clients[addr] = client
	}
	if err := client.Call(name, args, result); err != nil {
		if err := client.Close(); err != nil {
			log.Println(err)
		}
		delete(gateway.clients, addr)
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

func (gateway *RPCGateway) AppendEntriesMsg() <-chan *AppendEntriesMsg {
	return gateway.appendEntriesMsg
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

func (gateway *RPCGateway) RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error {
	return gateway.sendRPC("RPCGateway.RequestVoteRPC", id, args, result)
}

func (gateway *RPCGateway) RequestVoteMsg() <-chan *RequestVoteMsg {
	return gateway.requestVoteMsg
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
