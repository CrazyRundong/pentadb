package raft

// TODO
import (
	"bytes"
	"encoding/gob"
	// TODO: replace all bellow implementation
	cRaft "github.com/coreos/etcd/raft"
	cRaftPb "github.com/coreos/etcd/raft/raftpb"
	cRaftWal "github.com/coreos/etcd/wal"
)

type kvOp int
const (
	Get kvOp = iota
	Put
	Del
)

type OpLog struct {
	Op	kvOp
	Key string
	Val string
}

type raftNode struct {
	// channels for opLog throughput
	// chanPropose: come from kv-server
	chanPropose <-chan string
	// chanCommit & chanError: to kv-server
	chanCommit chan<- string
	chanError chan<- error

	id uint64
	peers []uint64  // ? maybe uint64
	node cRaft.Node
	nodeStorage *cRaft.MemoryStorage
	nodeWal *cRaftWal.WAL
}

func NewRaftNode(id uint64, peers []uint64, isJoin bool, chanPropose <-chan string) (chan<- string, chan<- error) {
	memStorage := cRaft.NewMemoryStorage()
	chanCommit := make(chan string)
	chanError := make(chan error)
	rNode := &raftNode{
		chanPropose: chanPropose,
		chanCommit: chanCommit,
		chanError: chanError,
		id: id,
		peers: peers,
		nodeStorage: memStorage,
	}

	// build cRaft.Node
	var cRaftClusterPeers []cRaft.Peer
	if isJoin {
		cRaftClusterPeers = nil
	} else {
		cRaftClusterPeers = make([]cRaft.Peer, len(rNode.peers))
		for i, p := range rNode.peers {
			cRaftClusterPeers[i] = cRaft.Peer{ID: p}
		}
	}
	c := &cRaft.Config{
		ID: rNode.id,
		Storage: rNode.nodeStorage,
	}
	rNode.node = cRaft.StartNode(c, cRaftClusterPeers)

	// TODO(Rundong): launch raft service

	return chanCommit, chanError
}

func (rn *raftNode) handlePropose() error {
	var op OpLog
	if err := gob.NewDecoder(&op).Decode(<-rn.chanPropose); err != nil {
		return err
	}
}

