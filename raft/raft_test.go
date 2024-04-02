package raft

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	const (
		numServers                 = 5
		discoveryPort              = 8000
		testTimeoutDuration        = 10 * time.Second
		sendCommandTimeoutDuration = 100 * time.Millisecond
		crashServerTimeoutDuration = time.Second
	)
	discoveryAddr := "localhost:" + strconv.Itoa(discoveryPort)
	var logger *slog.Logger = nil

	go func() {
		if err := NewDiscoveryService().Start(strconv.Itoa(discoveryPort)); err != nil {
			log.Fatal(err)
		}
	}()

	serverCommands := make([][]string, numServers)
	serverHandlers := make([]CommandHandler, numServers)
	servers := make([]*Server, numServers)

	startServerFunc := func(index int) {
		port := discoveryPort + index + 1
		portStr := strconv.Itoa(port)
		servers[index] = NewServer(ServerConfig{
			Id:      "localhost:" + portStr,
			Handler: serverHandlers[index],
			Logger:  logger,
		})
		go servers[index].Start(NewRPCGateway(portStr, discoveryAddr))
		log.Println("Started server at index", index)
	}

	for i := range numServers {
		{ // setup CommandHandler
			serverCommands[i] = make([]string, 0)
			logMutex := sync.Mutex{}
			serverHandlers[i] = func(command string) {
				logMutex.Lock()
				defer logMutex.Unlock()
				serverCommands[i] = append(serverCommands[i], command)
				log.Println("test")
			}
		}
		startServerFunc(i)
	}
	time.Sleep(time.Second)

	crashedServerIndex := -1
	expectedCommands := make([]string, 0)
	testTimeout := time.NewTimer(testTimeoutDuration)
	sendCommandTimeout := time.NewTimer(sendCommandTimeoutDuration)
	crashServerTimeout := time.NewTimer(crashServerTimeoutDuration)

testLoop:
	for {
		select {
		case <-testTimeout.C:
			if crashedServerIndex >= 0 {
				startServerFunc(crashedServerIndex)
			}
			break testLoop
		case <-sendCommandTimeout.C:
			input := CommandInput{
				Command: "Hello world: " + strconv.Itoa(len(expectedCommands)),
			}
			buffer, err := json.Marshal(&input)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.Post("http://"+discoveryAddr+"/command", "application/json", bytes.NewReader(buffer))
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("invalid status code of command post: %d", resp.StatusCode)
			}
			expectedCommands = append(expectedCommands, input.Command)
			sendCommandTimeout.Reset(sendCommandTimeoutDuration)
		case <-crashServerTimeout.C:
			if crashedServerIndex >= 0 {
				startServerFunc(crashedServerIndex)
			}
			i := rand.Intn(numServers)
			servers[i].Close()
			crashedServerIndex = i
			crashServerTimeout.Reset(crashServerTimeoutDuration)
			log.Println("Crashed server at index", i)
		}
	}

	time.Sleep(time.Second)
	for i := range numServers {
		servers[i].Close()
		log.Println("Closed server at index", i)
	}

	for _, commands := range serverCommands {
		assert.Equal(t, expectedCommands, commands)
	}
}
