package raft

// TODO
import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	cRaftStats "github.com/coreos/etcd/etcdserver/stats"
	cRaftFileUtil "github.com/coreos/etcd/pkg/fileutil"
	cRaftTypes "github.com/coreos/etcd/pkg/types"
	cRaft "github.com/coreos/etcd/raft"
	cRaftpb "github.com/coreos/etcd/raft/raftpb"
	cRaftHttp "github.com/coreos/etcd/rafthttp"
	cRaftSnap "github.com/coreos/etcd/snap"
	cRaftWal "github.com/coreos/etcd/wal"
	cRaftWalpb "github.com/coreos/etcd/wal/walpb"
)

type kvOp int

const (
	Get    kvOp = iota
	Put
	Del
	DoSnap
)

type OpLog struct {
	Op  kvOp
	Key string
	Val string
}

type raftNode struct {
	// channels for opLog throughput
	// chanPropose: come from kv-server
	chanPropose <-chan string
	// chanCommit & chanError: to kv-server
	chanCommit chan<- string
	chanError  chan<- error

	id                uint64
	peers             []uint64 // ? maybe uint64
	node              cRaft.Node
	nodeStorage       *cRaft.MemoryStorage
	committedEntryIdx uint64

	// entries for configuration state
	confState   cRaftpb.ConfState
	snapshotIdx uint64
	appliedIdx  uint64

	// TODO(Rundong): re-implement all bellow utils
	// use etcd.WAL to store Committed data
	// use etcd.snap.Snapshotter to store snapshots
	snapDir              string
	snapShotter          *cRaftSnap.Snapshotter
	chanSnapShotterReady chan *cRaftSnap.Snapshotter

	nodeWalDir string
	nodeWal    *cRaftWal.WAL

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
		chanPropose:       chanPropose,
		chanCommit:        chanCommit,
		chanError:         chanError,
		id:                id,
		peers:             peers,
		nodeStorage:       memStorage,
		committedEntryIdx: 0,
		snapDir:           fmt.Sprintf("kvNode_%d", id),
		nodeWalDir:        fmt.Sprintf("kvNode_%d_Wal", id),
	}

	// prepare local persistent storage
	if !cRaftFileUtil.Exist(rNode.snapDir) {
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
		ID:            rNode.id,
		Storage:       rNode.nodeStorage,
		ElectionTick:  10,
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
		ID:          cRaftTypes.ID(rNode.id),
		ClusterID:   0x001,
		Raft:        rNode,
		ServerStats: cRaftStats.NewServerStats("", ""),
		LeaderStats: cRaftStats.NewLeaderStats(strconv.FormatUint(rNode.id, 10)),
		ErrorC:      make(chan error),
	}

	// TODO(Rundong): launch raft service
	go rNode.runRaft()

	return chanCommit, chanError
}

func (rn *raftNode) prepareWal() (*cRaftWal.WAL, error) {
	var (
		snapShot  *cRaftpb.Snapshot
		w         *cRaftWal.WAL
		err       error
		hardState cRaftpb.HardState
		entries   []cRaftpb.Entry
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
		rn.committedEntryIdx = entries[len(entries)-1].Index
	} else {
		rn.chanCommit <- nil
	}

	return w, nil
}

func (rn *raftNode) openWAL(snapshot *cRaftpb.Snapshot) (*cRaftWal.WAL, error) {
	var (
		w   *cRaftWal.WAL
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

func (rn *raftNode) saveSnap(snp cRaftpb.Snapshot) error {
	walsnap := cRaftWalpb.Snapshot{
		Index: snp.Metadata.Index,
		Term:  snp.Metadata.Term,
	}
	if err := rn.nodeWal.SaveSnapshot(walsnap); err != nil {
		return err
	}
	if err := rn.snapShotter.SaveSnap(snp); err != nil {
		return err
	}
	return rn.nodeWal.ReleaseLockTo(snp.Metadata.Index)
}

func (rn *raftNode) publishSnapshot(snp cRaftpb.Snapshot) {
	if cRaft.IsEmptySnap(snp) {
		return
	}
	// TODO: maybe log here
	if snp.Metadata.Index <= rn.appliedIdx {
		log.Fatalf("raft node #%d: overdue snpshot with idx %d", rn.id, snp.Metadata.Index)
	}
	// prepare DoSnap log commit
	snpOp := OpLog{Op: DoSnap}
	buf := bytes.Buffer{}
	if err := gob.NewEncoder(&buf).Encode(snpOp); err != nil {
		log.Panic(err)
	}
	rn.chanCommit <- buf.String()

	rn.confState = snp.Metadata.ConfState
	rn.snapshotIdx = snp.Metadata.Index
	rn.appliedIdx = snp.Metadata.Index
}

func (rn *raftNode) runRaft() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	cPropError := make(chan error)
	go rn.handlePropose(cPropError)

	for {
		select {
		case <-ticker.C:
			rn.node.Tick()

		case rd := <-rn.node.Ready():
			rn.nodeWal.Save(rd.HardState, rd.Entries)
			if !cRaft.IsEmptySnap(rd.Snapshot) {
				rn.saveSnap(rd.Snapshot)
				rn.nodeStorage.ApplySnapshot(rd.Snapshot)
				rn.publishSnapshot(rd.Snapshot)
			}
			rn.nodeStorage.Append(rd.Entries)
			rn.transport.Send(rd.Messages)
			if err := rn.makeCommit(rn.entriesToApply(rd.Entries)); err != nil {
				rn.chanError <- err
				log.Fatalf("raft node #%d: %s", rn.id, err.Error())
			}
			// TODO: maybe trigger snapshot
			rn.node.Advance()

		case err := <-rn.transport.ErrorC:
			rn.chanError <- err
			// TODO: stop raft node

		case err := <-cPropError:
			rn.chanError <- err
		}
	}

}

func (rn *raftNode) handlePropose(cError chan<- error) {
	defer close(cError)
	for rn.chanPropose != nil {
		select {
		case prop, ok := <-rn.chanPropose:
			if !ok {
				rn.chanPropose = nil
				cError <- fmt.Errorf("raft node #%d: chanPropose closed", rn.id)
			} else {
				rn.node.Propose(context.TODO(), []byte(prop))
			}
			// TODO(Rundong) chanConfChange
		}
	}
}

func (rn *raftNode) makeCommit(ents []cRaftpb.Entry, err error) error {
	if err != nil {
		return err
	}
	for i, e := range ents {
		switch e.Type {
		case cRaftpb.EntryNormal:
			if len(e.Data) == 0 {
				break
			}
			s := string(e.Data)
			select {
			case rn.chanCommit <- s:
			default:
				return fmt.Errorf("invalid entry at idx:%d", i)
			}

		case cRaftpb.EntryConfChange:
			var cc cRaftpb.ConfChange
			cc.Unmarshal(e.Data)
			rn.confState = *rn.node.ApplyConfChange(cc)
			switch cc.Type {
			case cRaftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rn.transport.AddPeer(cRaftTypes.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case cRaftpb.ConfChangeRemoveNode:
				if cc.NodeID == rn.id {
					return fmt.Errorf("raft node #%d will offline", rn.id)
				}
				rn.transport.RemovePeer(cRaftTypes.ID(cc.NodeID))
			}
		}

		rn.appliedIdx = e.Index
		// TODO: handle commit finished, check last idx
	}
	return nil
}

func (rn *raftNode) entriesToApply(ents []cRaftpb.Entry) ([]cRaftpb.Entry, error) {
	if len(ents) == 0 {
		return nil, fmt.Errorf("no entries to apply")
	}
	if ents[0].Index > rn.appliedIdx+1 {
		return nil, fmt.Errorf("first entry idx #%d must follows last applied idx #%d", ents[0].Index, rn.appliedIdx)
	}
	if rn.appliedIdx-ents[0].Index+1 < uint64(len(ents)) {
		return ents[rn.appliedIdx-ents[0].Index+1:], nil
	}
	return ents, nil
}

func (rn *raftNode) Process(ctx context.Context, m cRaftpb.Message) error {
	return rn.node.Step(ctx, m)
}

func (rn *raftNode) IsIDRemoved(id uint64) bool {
	return false
}

func (rn *raftNode) ReportUnreachable(id uint64) {

}

func (rn *raftNode) ReportSnapshot(id uint64, status cRaft.SnapshotStatus) {

}
