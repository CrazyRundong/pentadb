// Contains the interface and implementation of Node

/* BSD 3-Clause License

Copyright (c) 2017, Guan Jiawen, Li Lundong
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package server

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"

	cRaftSnap "github.com/coreos/etcd/snap"
	"github.com/shenaishiren/pentadb/args"
	"github.com/shenaishiren/pentadb/log"
	"github.com/shenaishiren/pentadb/raft"
	"github.com/syndtr/goleveldb/leveldb"
)

var LOG = log.DefaultLog

type NodeStatus int

const (
	Running  NodeStatus = iota
	Terminal
)

type Node struct {
	HostIP       string
	State        NodeStatus
	OtherNodes   []string
	ReplicaNodes []string
	DB           *leveldb.DB
	mutex        *sync.RWMutex // read-write lock
	snapShotter  *cRaftSnap.Snapshotter

	// use these channels to communicate with raft cluster
	chanPropose       chan<- string
	chanCommit        <-chan *string
	chanError         <-chan error
	chanInternalError chan error
}

// TODO(Rundong): get bool: isJoin, []uint64: peers
// do peers check when join to a cluster, maybe check ont peer's
// `peers` filed?
func NewNode(hostIP, basePath string, isJoin bool, peers []string) (*Node, error) {
	var (
		err    error
		nodeId uint64
	)
	peersId := make([]uint64, len(peers))

	for i, p := range peers {
		peersId[i], err = ipToNodeId(p)
		if err != nil {
			return nil, err
		}
	}
	if nodeId, err = ipToNodeId(hostIP); err != nil {
		return nil, err
	}

	chanPropose := make(chan string)
	chanCommit, chanError, snapShotter := raft.NewRaftNode(nodeId, basePath, peersId, isJoin, chanPropose)

	return &Node{
		HostIP: hostIP,
		State:  Running,
		mutex:  new(sync.RWMutex),

		// bellow are channels for kv-node <-> Raft cluster
		chanPropose:       chanPropose,
		chanCommit:        chanCommit,
		chanError:         chanError,
		chanInternalError: make(chan error),
		snapShotter:       <-snapShotter,
	}, nil
}

func (n *Node) randomChoice(list []string, k int) []string {
	pool := list
	p := len(pool)
	result := make([]string, k)
	for i := 0; i < k; i++ {
		j := rand.Intn(p - i)
		result[i] = pool[j]
		pool[j] = result[p-i-1]
	}
	return result
}

func (n *Node) Init(args *args.InitArgs, result *[]byte) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.HostIP = args.Self
	n.OtherNodes = args.OtherNodes
	replicaNodes := n.randomChoice(args.OtherNodes, args.Replicas)
	if len(replicaNodes) == 0 {
		return errors.New(fmt.Sprintf("node %s init failed", n.HostIP))
	}
	replicaNodes = append(replicaNodes, n.HostIP)
	n.ReplicaNodes = replicaNodes
	return nil
}

func (n *Node) AddNode(node string, result *[]byte) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.OtherNodes = append(n.OtherNodes, node)
	return nil
}

func (n *Node) RemoveNode(node string, result *[]byte) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	var j = 0
	var flag = false
	for _, otherNode := range n.OtherNodes {
		if otherNode != node {
			n.OtherNodes[j] = otherNode
			j++
		} else {
			flag = true
		}
	}
	if flag {
		n.OtherNodes = n.OtherNodes[0:len(n.OtherNodes)-1]
	}
	return nil
}

func (n *Node) Put(args *args.KVArgs, result *[]byte) error {
	var (
		err    error
		putLog raft.OpLog
		logBuf bytes.Buffer
	)

	putLog = raft.OpLog{Op: raft.Put, Key: args.Key, Val: args.Value}
	if err = gob.NewEncoder(&logBuf).Encode(putLog); err != nil {
		return err
	}

	n.chanPropose <- logBuf.String()

	return nil
}

func (n *Node) Get(key []byte, result *[]byte) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	res, err := n.DB.Get(key, nil)
	*result = res
	return err
}

func (n *Node) Delete(key string, result *[]byte) error {
	var (
		err    error
		delLog raft.OpLog
		logBuf bytes.Buffer
	)

	delLog = raft.OpLog{Op: raft.Del, Key: key, Val: ""}
	if err = gob.NewEncoder(&logBuf).Encode(delLog); err != nil {
		return err
	}

	n.chanPropose <- logBuf.String()

	return nil
}

func (n *Node) HandleCommit() {
	for data := range n.chanCommit {
		var kvOp raft.OpLog
		if err := gob.NewDecoder(bytes.NewBufferString(*data)).Decode(&kvOp); err != nil {
			n.chanInternalError <- err
		}
		switch kvOp.Op {
		case raft.Get:
		case raft.Put:
			n.mutex.Lock()
			n.DB.Put([]byte(kvOp.Key), []byte(kvOp.Val), nil)
			n.mutex.Unlock()
		case raft.Del:
			n.mutex.Lock()
			n.DB.Delete([]byte(kvOp.Key), nil)
			n.mutex.Unlock()
		case raft.DoSnap:
			// TODO: handle snapshot
		default:
			err := errors.New(fmt.Sprintf("Invalid operation (%v) in Node %s", kvOp, n.HostIP))
			n.chanInternalError <- err
		}
	}
}

func (n *Node) HandleError() {
	var (
		err error
	)
	for {
		select {
		case err = <-n.chanError:
			LOG.Errorf("kv-storage get error from raft node: %s", err)
		case err = <-n.chanInternalError:
			LOG.Errorf("kv-storage get error from raft node: %s", err)
		}
	}
}

func (n *Node) doSnapshot() ([]byte, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	return json.Marshal(n.DB)
}

func ipToNodeId(ipAdd string) (uint64, error) {
	ipToken := strings.Split(ipAdd, ":")
	ip := net.ParseIP(ipToken[0])
	port, err := strconv.Atoi(ipToken[1])
	if ip == nil || err != nil {
		return 0, errors.New(fmt.Sprintf("Can't parse ip add: %s", ipAdd))
	}

	var nodeId uint64
	ipNum := ip2int(ip)
	nodeId = uint64(ipNum)<<32 + uint64(port)

	return nodeId, nil
}

// thanks to: https://gist.github.com/ammario/649d4c0da650162efd404af23e25b86b
func ip2int(ip net.IP) uint32 {
	if len(ip) == 16 {
		return binary.BigEndian.Uint32(ip[12:16])
	}
	return binary.BigEndian.Uint32(ip)
}

func int2ip(nn uint32) net.IP {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, nn)
	return ip
}
