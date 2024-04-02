package raft

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"
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

	servers := make([]*Server, numServers)
	for i := range numServers {
		port := discoveryPort + i + 1
		portStr := strconv.Itoa(port)
		config := ServerConfig{
			Id:     "localhost:" + portStr,
			Logger: logger,
		}
		servers[i] = NewServer(config)
		go servers[i].Start(NewRPCGateway(portStr, discoveryAddr))
	}
	time.Sleep(time.Second)

	crashedServerIndex := -1
	testTimeout := time.NewTimer(testTimeoutDuration)
	sendCommandTimeout := time.NewTimer(sendCommandTimeoutDuration)
	crashServerTimeout := time.NewTimer(crashServerTimeoutDuration)

testLoop:
	for {
		select {
		case <-testTimeout.C:
			break testLoop
		case <-sendCommandTimeout.C:
			input := CommandInput{
				Command: "Hello world",
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
			sendCommandTimeout.Reset(sendCommandTimeoutDuration)
		case <-crashServerTimeout.C:
			if crashedServerIndex >= 0 {
				port := discoveryPort + crashedServerIndex + 1
				portStr := strconv.Itoa(port)
				servers[crashedServerIndex] = NewServer(ServerConfig{
					Id:     "localhost:" + portStr,
					Logger: logger,
				})
				go servers[crashedServerIndex].Start(NewRPCGateway(portStr, discoveryAddr))
			}
			i := rand.Intn(numServers)
			servers[i].Close()
			crashedServerIndex = i
			crashServerTimeout.Reset(crashServerTimeoutDuration)
		}
	}

	for i := range numServers {
		servers[i].Close()
		log.Println("Closed server at index", i)
	}
}
