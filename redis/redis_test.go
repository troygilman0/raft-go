package redis

import (
	"log"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/troygilman0/raft-go/raft"
)

func TestRedis(t *testing.T) {
	const (
		discoveryPort      = 8000
		testDuration       = 10 * time.Second
		setTimeoutDuration = 5 * time.Millisecond
		getTimeoutDuration = 7 * time.Millisecond
	)
	discoveryAddr := "localhost:" + strconv.Itoa(discoveryPort)

	go func() {
		if err := raft.NewDiscoveryService(slog.Default()).Start(strconv.Itoa(discoveryPort)); err != nil {
			log.Fatal(err)
		}
	}()

	for i := range 5 {
		server := NewServer(ServerConfig{
			Host:          "localhost",
			Port:          strconv.Itoa(discoveryPort + i + 1),
			DiscoveryAddr: discoveryAddr,
		})
		go server.Start()
	}

	client := NewClient(ClientConfig{
		Addr: "http://" + discoveryAddr,
	})

	timeout := time.NewTimer(testDuration)
	setTimeout := time.NewTimer(setTimeoutDuration)
	getTimeout := time.NewTimer(getTimeoutDuration)
	var val int = 0

testLoop:
	for {
		select {
		case <-timeout.C:
			break testLoop
		case <-setTimeout.C:
			setTimeout.Reset(setTimeoutDuration)
			val++
			if err := client.Set("val", &val); err != nil {
				t.Fatal(err)
			}
		case <-getTimeout.C:
			getTimeout.Reset(getTimeoutDuration)
			var val0 int
			if err := client.Get("val", &val0); err != nil {
				t.Fatal(err)
			}
			if val0 != val {
				t.Fatalf("Get action returned the incorrect val: %d != %d", val, val0)
			}
		}
	}
}
