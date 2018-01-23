// Contains the interface and implementation of PentaDB Client

/* BSD 3-Clause License

Copyright (c) 2017, Ruiyang Ding
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
package client

import(
	"net"
	"net/rpc/jsonrpc"
	"net/rpc"
	"testing"
	"fmt"
)

func Start(t *testing.T) {
	var nodes = []string{
		"127.0.0.1:4567",
	}
	client, err := NewClient(nodes, nil, 1)
	if err != nil {
		t.Error(err.Error())
		return
	}
	rpc.Register(client)
	l, err := net.Listen("tcp", ":3333")
	if err != nil{
		fmt.Printf("Listener tcp err; %s", err)
		return
	}
	for{
		fmt.Println("Waiting")
		conn, err := l.Accept()
		if err != nil{
			fmt.Sprintf("Accept connextion err")
		}
		go jsonrpc.ServeConn(conn)
	}
}

