package redis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/troygilman0/raft-go/raft"
)

type ClientConfig struct {
	Addr string
}

type Client struct {
	config ClientConfig
}

func NewClient(config ClientConfig) Client {
	return Client{
		config: config,
	}
}

func (client *Client) Get(key string, val interface{}) error {
	body := raft.CommandInput{
		Command: fmt.Sprintf("%s %s", commandTypeGet, key),
	}

	bodyBuffer, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := http.Post(client.config.Addr+"/command", "application/json", bytes.NewReader(bodyBuffer))
	if err != nil {
		return err
	}

	return json.NewDecoder(resp.Body).Decode(val)
}

func (client *Client) Set(key string, val interface{}) error {
	valBuffer, err := json.Marshal(val)
	if err != nil {
		return err
	}

	body := raft.CommandInput{
		Command: fmt.Sprintf("%s %s %s", commandTypeSet, key, string(valBuffer)),
	}

	bodyBuffer, err := json.Marshal(body)
	if err != nil {
		return err
	}

	_, err = http.Post(client.config.Addr+"/command", "application/json", bytes.NewReader(bodyBuffer))
	if err != nil {
		return err
	}

	return nil
}
