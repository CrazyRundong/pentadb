package opt

import "time"

const (
	DefaultReplicas = 0                 // default replicas for raft algorithm
	DeafultPath = "/tmp/pentadb"        // default path for levelDB
	DefaultProtocol = "tcp"
	DefaultTimeout = 3 * time.Second
)

type NodeState int

const (
	NodeRunning NodeState = iota
	NodeTerminal
)

type NodeProxyOptions struct {
	// node's replicas
	Replicas int

	// Whether to force the flush to the node
	Flush bool
}