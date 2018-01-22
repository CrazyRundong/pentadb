// Contains the implementation of server-command of levelDB

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

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"

	pLog "github.com/shenaishiren/pentadb/log"
	"github.com/shenaishiren/pentadb/opt"
	"github.com/shenaishiren/pentadb/rpc"
	"github.com/shenaishiren/pentadb/server"
	"github.com/syndtr/goleveldb/leveldb"
)

var LOG = pLog.DefaultLog

var helpPrompt = `Usage: pentadb [--port <port>] [--path <path>] [options]

A PentaDB rpc server, backed by LevelDB

Options:
	--help           		Display this help message and exit
	--port <port>    		The port to listen on (default: 4567)
	--path <path>    		The path to use for the LevelDB store
`

type Server struct {
	Nodes []*server.Node
}

func (s *Server) listen(hostIP string, isJoin bool, peers []string, path string) {
	var (
		err error
		n   *server.Node
	)
	port := strings.Split(hostIP, ":")[1]

	if n, err = server.NewNode(hostIP, path, isJoin, peers); err != nil {
		log.Panicf("fail to initalize reft server node: %s", err.Error())
	}

	db, err := leveldb.OpenFile(path+"/localDB", nil)
	if err != nil {
		LOG.Error("open levelDB error: ", err.Error())
		return
	}
	n.DB = db

	rpc.Register(n)
	s.Nodes = append(s.Nodes, n)

	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		LOG.Error("listen error: ", err.Error())
		return
	}

	go func() {
		LOG.Infof("listen at 0.0.0.0:%s", port)
		for {
			conn, err := l.Accept()
			if err != nil {
				LOG.Error("accept rpc connection", err.Error())
				continue
			}
			// blocking
			go rpc.ServeConn(conn)
		}
	}()
}

func main() {
	var (
		help   bool
		isJoin bool
		peers  string
		port   string
		path   string
	)
	flag.BoolVar(&help, "h", false, "Display this help message and exit")
	flag.BoolVar(&isJoin, "j", false, "whether to join another running cluster")
	flag.StringVar(&peers, "p", "", "ip:port of peers, separated by `,`")
	flag.StringVar(&port, "P", "4567", "The port to listen on (default: 4567),"+
		"separated by `,` if launch multiple server nodes on one host")
	flag.StringVar(&path, "a", opt.DeafultPath, "The path to use for the LevelDB store")

	// change default usage
	flag.Usage = func() {
		fmt.Println(helpPrompt)
	}

	// run
	flag.Parse()

	// help command
	if help {
		fmt.Print(helpPrompt)
	} else {
		var (
			hostIPs   []net.IP
			hostPorts []string
			peerURLs  []string
			hostURLs  []string
			err       error
		)

		if hostIPs, err = server.GetMyIP(); err != nil {
			LOG.Errorf("fail to get server host ip: %s", err.Error())
		}

		hostPorts = strings.Split(port, ",")
		for _, p := range hostPorts {
			hostURLs = append(hostURLs, hostIPs[0].String()+":"+p)
		}

		if isJoin {
			peerURLs = strings.Split(peers, ",")
		}

		svr := new(Server)
		for _, u := range hostURLs {
			svr.listen(u, isJoin, peerURLs, path)
		}
	}
}
