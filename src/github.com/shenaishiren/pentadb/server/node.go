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
	"sync"
	"errors"
	"math/rand"
	"fmt"
	"bytes"
	"net"
	"encoding/gob"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/shenaishiren/pentadb/args"
	"github.com/shenaishiren/pentadb/log"
	"github.com/shenaishiren/pentadb/raft"
	"strings"
	"strconv"
)

var LOG = log.DefaultLog

type NodeStatus int

const (
	Running  NodeStatus = iota
	Terminal
)

type Node struct {
	Ipaddr string

	State NodeStatus

	OtherNodes []string

	ReplicaNodes []string

	DB *leveldb.DB

	mutex *sync.RWMutex // read-write lock

	// use these channels to communicate with raft cluster
	chanPropose <-chan string
	// load from raft cluster
	chanCommit chan<- string
	chanError  chan<- error
}

// TODO(Rundong): get bool: isJoin, []uint64: peers
// do peers check when join to a cluster, maybe check ont peer's
// `peers` filed?
func NewNode(ipaddr string, isJoin bool, peers []string) (*Node, error) {
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
	if nodeId, err = ipToNodeId(ipaddr); err != nil {
		return nil, err
	}

	chanPropose := make(chan string)
	chanCommit, chanError := raft.NewRaftNode(nodeId, peersId, isJoin, chanPropose)

	return &Node{
		Ipaddr: ipaddr,
		State:  Running,
		mutex:  new(sync.RWMutex),

		// bellow are channels for kv-node <-> Raft cluster
		chanPropose: chanPropose,
		chanCommit:  chanCommit,
		chanError:   chanError,
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

	n.Ipaddr = args.Self
	n.OtherNodes = args.OtherNodes
	replicaNodes := n.randomChoice(args.OtherNodes, args.Replicas)
	if len(replicaNodes) == 0 {
		return errors.New(fmt.Sprintf("node %s init failed", n.Ipaddr))
	}
	replicaNodes = append(replicaNodes, n.Ipaddr)
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
	n.mutex.Lock()
	defer n.mutex.Unlock()

	err := n.DB.Put(args.Key, args.Value, nil)
	return err
}

func (n *Node) Get(key []byte, result *[]byte) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	res, err := n.DB.Get(key, nil)
	*result = res
	return err
}

func (n *Node) Delete(key []byte, result *[]byte) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	err := n.DB.Delete(key, nil)
	return err
}

func (rn *Node) Propose(op raft.OpLog) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
		return err
	}
	rn.chanPropose <- buf.String()
	return nil
}

func (rn *Node) handleCommit() error {
	for data := range rn.chanCommit {
		var kvOp raft.OpLog
		if err := gob.NewDecoder(bytes.NewBufferString(data)).Decode(&kvOp); err != nil {
			return err
		}
		switch kvOp.Op {
		case raft.Get:
		case raft.Put:
			rn.mutex.Lock()
			rn.DB.Put([]byte(kvOp.Key), []byte(kvOp.Val), nil)
			rn.mutex.Unlock()
		case raft.Del:
			rn.mutex.Lock()
			rn.DB.Delete([]byte(kvOp.Key), nil)
			rn.mutex.Unlock()
		default:
			return errors.New(fmt.Sprintf("Invalid operation (%v) in Node %s", kvOp, rn.Ipaddr))
		}
	}
	defer close(rn.chanCommit)
	return nil
}

func (rn *Node) handleError() error {
	// TODO
	for err := range rn.chanError {

	}
	defer close(rn.chanError)
	return nil
}

func ipToNodeId(ipAdd string) (uint64, error) {
	ipToken := strings.Split(ipAdd, ":")
	ip := net.ParseIP(ipToken[0])
	port, err := strconv.Atoi(ipToken[1])
	if ip == nil || err != nil {
		return -1, errors.New(fmt.Sprintf("Can't parse ip add: %s", ipAdd))
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
