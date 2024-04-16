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
	Messages() <-chan Message
	Start() error
	Close() error
}

func NewRPCGateway(port string, discoveryAddr string) Gateway {
	return &RPCGateway{
		httpServer:    &http.Server{Addr: ":" + port},
		discoveryAddr: discoveryAddr,
		clients:       &sync.Map{},
		messages:      make(chan Message, 1000),
	}
}

type RPCGateway struct {
	httpServer    *http.Server
	discoveryAddr string
	clients       *sync.Map
	messages      chan Message
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

func sendRPC(clients *sync.Map, name string, addr string, args any, result any) error {
	var client *rpc.Client
	{ // find or create a client
		clientAny, ok := clients.Load(addr)
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
			clients.Store(addr, client)
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
			clients.Delete(addr)
		}
	}
	return err
}

func (gateway *RPCGateway) Discover(args *DiscoverArgs, result *DiscoverResult) error {
	return sendRPC(gateway.clients, "DiscoveryService.DiscoverRPC", gateway.discoveryAddr, args, result)
}

func (gateway *RPCGateway) AppendEntries(id string, args *AppendEntriesArgs, result *AppendEntriesResult) error {
	return sendRPC(gateway.clients, "RPCGateway.AppendEntriesRPC", id, args, result)
}

func (gateway *RPCGateway) RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error {
	return sendRPC(gateway.clients, "RPCGateway.RequestVoteRPC", id, args, result)
}

func (gateway *RPCGateway) AppendEntriesRPC(args *AppendEntriesArgs, result *AppendEntriesResult) error {
	msg := newMessageImpl()
	gateway.messages <- AppendEntriesMsg{
		Message: msg,
		args:    args,
		result:  result,
	}
	<-msg.done
	return nil
}

func (gateway *RPCGateway) RequestVoteRPC(args *RequestVoteArgs, result *RequestVoteResult) error {
	msg := newMessageImpl()
	gateway.messages <- RequestVoteMsg{
		Message: msg,
		args:    args,
		result:  result,
	}
	<-msg.done
	return nil
}

func (gateway *RPCGateway) CommandRPC(args *CommandArgs, result *CommandResult) error {
	msg := newMessageImpl()
	gateway.messages <- CommandMsg{
		Message: msg,
		Args:    args,
		Result:  result,
	}
	<-msg.done
	return nil
}

func (gateway *RPCGateway) Messages() <-chan Message {
	return gateway.messages
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
			case msg := <-gateway.messages:
				msg.Done()
			default:
				break flushLoop
			}
		}
	}
	return nil
}
