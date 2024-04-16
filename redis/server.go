package redis

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"

	"github.com/troygilman0/raft-go/raft"
)

type ServerConfig struct {
	Host          string
	Port          string
	DiscoveryAddr string
}

type Server struct {
	config ServerConfig
	values map[string]interface{}
	mutex  sync.RWMutex
}

func NewServer(config ServerConfig) *Server {
	return &Server{
		config: config,
		values: make(map[string]interface{}),
	}
}

func (server *Server) Start() {
	config := raft.ServerConfig{
		Id: server.config.Host + ":" + server.config.Port,
		Handler: func(command string) {
			t, key, val := parseCommand(command)
			if t == commandTypeSet {
				server.mutex.Lock()
				defer server.mutex.Unlock()
				server.values[key] = val
			}
		},
		Middleware: func(command string) (interface{}, bool) {
			t, key, _ := parseCommand(command)
			if t == commandTypeGet {
				server.mutex.RLock()
				defer server.mutex.RUnlock()
				return server.values[key], true
			}
			return nil, false
		},
		Logger: slog.Default(),
	}
	raft.NewServer(config).Start(raft.NewRPCTransport(server.config.Port, server.config.DiscoveryAddr))
}

func parseCommand(command string) (t commandType, key string, val interface{}) {
	commandSplit := strings.Split(command, " ")
	if len(commandSplit) > 0 {
		t = commandType(commandSplit[0])
	}
	if len(commandSplit) > 1 {
		key = commandSplit[1]
	}
	if len(commandSplit) > 2 {
		json.NewDecoder(bytes.NewReader([]byte(commandSplit[2]))).Decode(&val)
	}
	return t, key, val
}

type commandType string

const (
	commandTypeGet commandType = "GET"
	commandTypeSet commandType = "SET"
)
