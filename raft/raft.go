package raft

// TODO
import (
	"bytes"
	"encoding/gob"
	"os"
	"log"
	"fmt"
	cRaft "github.com/coreos/etcd/raft"
	cRaftpb "github.com/coreos/etcd/raft/raftpb"
	cRaftWal "github.com/coreos/etcd/wal"
	cRaftWalpb "github.com/coreos/etcd/wal/walpb"
	cRaftSnap "github.com/coreos/etcd/snap"
	cRaftSnappb "github.com/coreos/etcd/snap/snappb"
	cRaftHttp "github.com/coreos/etcd/rafthttp"
	cRaftFileUtil "github.com/coreos/etcd/pkg/fileutil"
	cRaftTypes "github.com/coreos/etcd/pkg/types"
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
	committedEntryIdx uint64

	// TODO(Rundong): re-implement all bellow utils
	// use etcd.WAL to store Committed data
	// use etcd.snap.Snapshotter to store snapshots
	snapDir string
	snapShotter *cRaftSnap.Snapshotter
	chanSnapShotterReady chan *cRaftSnap.Snapshotter

	nodeWalDir string
	nodeWal *cRaftWal.WAL

	// use etcd.httpraft to communicates though raft cluster
	transport *cRaftHttp.Transport
}

func NewRaftNode(id uint64, peers []uint64, isJoin bool, chanPropose <-chan string) (<-chan string, <-chan error) {
	var (
		err error
	)

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
		committedEntryIdx: 0,
		snapDir: fmt.Sprintf("kvNode_%d", id),
		nodeWalDir: fmt.Sprintf("kvNode_%d_Wal", id),
	}

	// prepare local persistent storage
	if ! cRaftFileUtil.Exist(rNode.snapDir) {
		if err := os.Mkdir(rNode.snapDir, 0750); err != nil {
			log.Fatalf("Can't create snapshot dir for node %d: %s", id, rNode.snapDir)
		}
	}
	rNode.snapShotter = cRaftSnap.New(rNode.snapDir)
	rNode.chanSnapShotterReady <- rNode.snapShotter

	isOldWalExists := cRaftWal.Exist(rNode.nodeWalDir)
	if rNode.nodeWal, err = rNode.prepareWal(); err != nil {
		log.Fatal(err)
	}

	// build cRaft.Node
	var cRaftClusterPeers []cRaft.Peer
	c := &cRaft.Config{
		ID: rNode.id,
		Storage: rNode.nodeStorage,
		ElectionTick: 10,
		HeartbeatTick: 1,
	}

	if isOldWalExists {
		rNode.node = cRaft.RestartNode(c)
	} else {
		if isJoin {
			cRaftClusterPeers = nil
		} else {
			cRaftClusterPeers = make([]cRaft.Peer, len(rNode.peers))
			for i, p := range rNode.peers {
				cRaftClusterPeers[i] = cRaft.Peer{ID: p}
			}
		}
		rNode.node = cRaft.StartNode(c, cRaftClusterPeers)
	}

	// launch HTTP transport between nodes
	rNode.transport = &cRaftHttp.Transport{
		ID: cRaftTypes.ID(rNode.id),
		ClusterID: 0x001,
		Raft: rNode,  // TODO(Rundong): implement interface http.Raft

	}

	// TODO(Rundong): launch raft service
	go rNode.runRaft()

	return chanCommit, chanError
}

func (rn *raftNode) prepareWal() (*cRaftWal.WAL, error) {
	var (
		snapShot *cRaftpb.Snapshot
		w *cRaftWal.WAL
		err error
		hardState cRaftpb.HardState
		entries []cRaftpb.Entry
	)
	if snapShot, err = rn.snapShotter.Load(); err != nil && err != cRaftSnap.ErrNoSnapshot {
		return nil, err
	}
	if w, err = rn.openWAL(snapShot); err != nil {
		return nil, err
	}
	if _, hardState, entries, err = w.ReadAll(); err != nil {
		return nil, err
	}

	// save WAL data to memory storage
	if snapShot != nil {
		rn.nodeStorage.ApplySnapshot(*snapShot)
	}
	rn.nodeStorage.SetHardState(hardState)
	rn.nodeStorage.Append(entries)

	// update index of entries
	if len(entries) > 0 {
		rn.committedEntryIdx = entries[len(entries) - 1].Index
	} else {
		rn.chanCommit <- nil
	}

	return w, nil
}

func (rn *raftNode) openWAL(snapshot *cRaftpb.Snapshot) (*cRaftWal.WAL, error) {
	var (
		w *cRaftWal.WAL
		err error
	)
	if !cRaftWal.Exist(rn.nodeWalDir) {
		if err = os.Mkdir(rn.nodeWalDir, 0750); err != nil {
			return nil, err
		}

		if w, err = cRaftWal.Create(rn.nodeWalDir, nil); err != nil {
			return nil, err
		}

		w.Close()
	}

	// if snapshot, store it
	walSnapShot := cRaftWalpb.Snapshot{}
	if snapshot != nil {
		walSnapShot.Index, walSnapShot.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	if w, err = cRaftWal.Open(rn.nodeWalDir, walSnapShot); err != nil {
		return nil, err
	}

	return w, nil
}

func (rn *raftNode) runRaft() {
	for {
		select {
		case nodeReady := <-rn.node.Ready():
		}

	}

}

func (rn *raftNode) handlePropose() error {
	// TODO(Rundong)
	var op OpLog
	if err := gob.NewDecoder(&op).Decode(<-rn.chanPropose); err != nil {
		return err
	}
}

// interfaces for etcd.rafthttp
