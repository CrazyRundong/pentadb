// This is test file for client.go

package client

import (
	"testing"
)

//func TestNewClient_NoEnoughNodes(t *testing.T) {
//	var nodes = []string{"127.0.0.1:5000"}
//
//	if _, err := NewClient(nodes, nil, 3); err != nil {
//		t.Errorf(err.Error())
//	}
//}

func TestNewClient(t *testing.T) {
	var nodes = []string{
		"127.0.0.1:4567",
	}
	client, err := NewClient(nodes, nil, 1)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer client.Close()
	if len(client.nodes) != len(nodes) {
		t.Error("wrong node number")
	}

	var (
		testKey = "key_1"
		testVal = "val_1"
	)
	client.Put(testKey, testVal)
	if value := client.Get(testKey); value != testVal {
		t.Errorf("get %s, expect %s", value, testVal)
	} else {
		LOG.Debug("value: ", value)
	}
}
