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

type Transport interface {
	Discover(args *DiscoverArgs, result *DiscoverResult) error
	AppendEntries(id string, args *AppendEntriesArgs, result *AppendEntriesResult) error
	RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error
	Messages() <-chan Message
	Start() error
	Close() error
}

func NewRPCTransport(port string, discoveryAddr string) Transport {
	return &RPCTransport{
		httpServer:    &http.Server{Addr: ":" + port},
		discoveryAddr: discoveryAddr,
		clients:       &sync.Map{},
		messages:      make(chan Message, 1000),
	}
}

type RPCTransport struct {
	httpServer    *http.Server
	discoveryAddr string
	clients       *sync.Map
	messages      chan Message
}

func (t *RPCTransport) Start() error {
	rpcServer, err := newRPCServer(t)
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/rpc", rpcServer)
	t.httpServer.Handler = mux
	if err := t.httpServer.ListenAndServe(); err != http.ErrServerClosed {
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

func (t *RPCTransport) Discover(args *DiscoverArgs, result *DiscoverResult) error {
	return sendRPC(t.clients, "DiscoveryService.DiscoverRPC", t.discoveryAddr, args, result)
}

func (t *RPCTransport) AppendEntries(id string, args *AppendEntriesArgs, result *AppendEntriesResult) error {
	return sendRPC(t.clients, "RPCTransport.AppendEntriesRPC", id, args, result)
}

func (t *RPCTransport) RequestVote(id string, args *RequestVoteArgs, result *RequestVoteResult) error {
	return sendRPC(t.clients, "RPCTransport.RequestVoteRPC", id, args, result)
}

func (t *RPCTransport) AppendEntriesRPC(args *AppendEntriesArgs, result *AppendEntriesResult) error {
	msg := newMessageImpl()
	t.messages <- AppendEntriesMsg{
		Message: msg,
		args:    args,
		result:  result,
	}
	<-msg.done
	return nil
}

func (t *RPCTransport) RequestVoteRPC(args *RequestVoteArgs, result *RequestVoteResult) error {
	msg := newMessageImpl()
	t.messages <- RequestVoteMsg{
		Message: msg,
		args:    args,
		result:  result,
	}
	<-msg.done
	return nil
}

func (t *RPCTransport) CommandRPC(args *CommandArgs, result *CommandResult) error {
	msg := newMessageImpl()
	t.messages <- CommandMsg{
		Message: msg,
		Args:    args,
		Result:  result,
	}
	<-msg.done
	return nil
}

func (t *RPCTransport) Messages() <-chan Message {
	return t.messages
}

func (t *RPCTransport) Close() error {
	{ // close clients
		t.clients.Range(func(key, value any) bool {
			client, ok := value.(*rpc.Client)
			if ok {
				client.Close()
			}
			return true
		})
	}
	{ // close http server
		if err := t.httpServer.Close(); err != nil {
			return err
		}
	}
	{ // flush channels
	flushLoop:
		for {
			select {
			case msg := <-t.messages:
				msg.Done()
			default:
				break flushLoop
			}
		}
	}
	return nil
}
