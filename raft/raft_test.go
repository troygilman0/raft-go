package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
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
		sendCommandTimeoutDuration = 10 * time.Millisecond
		crashServerTimeoutDuration = time.Second
	)
	discoveryAddr := "localhost:" + strconv.Itoa(discoveryPort)

	out, err := os.OpenFile("test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var logger *slog.Logger = slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{}))

	go func() {
		if err := NewDiscoveryService(logger).Start(strconv.Itoa(discoveryPort)); err != nil {
			log.Fatal(err)
		}
	}()

	serverCommands := make([][]string, numServers)
	serverHandlers := make([]CommandHandler, numServers)
	servers := make([]*Server, numServers)

	startServerFunc := func(index int) {
		port := discoveryPort + index + 1
		portStr := strconv.Itoa(port)
		serverCommands[index] = make([]string, 0)
		servers[index] = NewServer(ServerConfig{
			Id:      "localhost:" + portStr,
			Handler: serverHandlers[index],
			Logger:  logger,
		})
		go servers[index].Start(NewRPCGateway(portStr, discoveryAddr))
	}

	for i := range numServers {
		{ // setup CommandHandler
			logMutex := &sync.Mutex{}
			serverHandlers[i] = func(command string) {
				logMutex.Lock()
				defer logMutex.Unlock()
				serverCommands[i] = append(serverCommands[i], command)
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
	errChan := make(chan error)

testLoop:
	for {
		select {
		case <-testTimeout.C:
			if crashedServerIndex >= 0 {
				startServerFunc(crashedServerIndex)
			}
			break testLoop
		case <-sendCommandTimeout.C:
			sendCommandTimeout.Reset(sendCommandTimeoutDuration)
			input := CommandInput{
				Command: "Hello world: " + strconv.Itoa(len(expectedCommands)+1),
			}
			expectedCommands = append(expectedCommands, input.Command)
			buffer, err := json.Marshal(&input)
			if err != nil {
				t.Fatal(err)
			}
			go func(r io.Reader) {
				resp, err := http.Post("http://"+discoveryAddr+"/command", "application/json", r)
				if err != nil {
					errChan <- err
					return
				}
				if resp.StatusCode != http.StatusOK {
					errChan <- fmt.Errorf("invalid status code of command post: %d", resp.StatusCode)
				}
			}(bytes.NewReader(buffer))
		case <-crashServerTimeout.C:
			crashServerTimeout.Reset(crashServerTimeoutDuration)
			if crashedServerIndex >= 0 {
				startServerFunc(crashedServerIndex)
			}
			i := crashedServerIndex
			for i == crashedServerIndex {
				i = rand.Intn(numServers)
			}
			servers[i].Close()
			crashedServerIndex = i
		case err := <-errChan:
			t.Fatal(err)
		}
	}

	time.Sleep(10 * time.Second)
	for i := range numServers {
		servers[i].Close()
	}

	for _, commands := range serverCommands {
		assert.Equal(t, expectedCommands, commands)
	}
}
