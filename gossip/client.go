// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"encoding/gob"
	"net"
	"time"

	"gossipgo/rpc"
	"gossipgo/util"
	"log"
)

const (
	// maxWaitForNewGossip is minimum wait for new gossip before a
	// peer is considered a poor source of good gossip and is GC'd.
	maxWaitForNewGossip = 10 * time.Second
	// gossipDialTimeout is timeout for net.Dial call to connect to
	// a gossip server.
	gossipDialTimeout = 2 * time.Second
)

// init pre-registers net.UnixAddr and net.TCPAddr concrete types with
// gob. If other implementations of net.Addr are passed, they must be
// added here as well.
func init() {
	gob.Register(&net.TCPAddr{})
	gob.Register(&net.UnixAddr{})
}

// client is a client-side RPC connection to a gossip peer node.
type client struct {
	addr        net.Addr      // Peer node network address
	rpcClient   *rpc.Client   // RPC client
	forwardAddr net.Addr      // Set if disconnected with an alternate addr
	lastFresh   int64         // Last wall time client received fresh info
	err         error         // Set if client experienced an error
	closer      chan struct{} // Client shutdown channel
}

// newClient creates and returns a client struct.
func newClient(addr net.Addr) *client {
	return &client{
		addr:   addr,
		closer: make(chan struct{}, 1),
	}
}

// start dials the remote addr and commences gossip once connected.
// Upon exit, signals client is done by pushing it onto the done
// channel. If the client experienced an error, its err field will
// be set. This method blocks and should be invoked via goroutine.
func (c *client) start(g *Gossip, done chan *client) {
	c.rpcClient = rpc.NewClient(c.addr)
	select {
	case <-c.rpcClient.Ready:
		// Start gossip; see below.
	case <-time.After(gossipDialTimeout):
		c.err = util.Errorf("timeout connecting to remote server: %v", c.addr)
		done <- c
		return
	}

	// Start gossipping and wait for disconnect or error.
	c.lastFresh = time.Now().UnixNano()
	err := c.gossip(g)
	if err != nil {
		c.err = util.Errorf("gossip client: %s", err)
	}
	done <- c
}

// close stops the client gossip loop and returns immediately.
func (c *client) close() {
	close(c.closer)
}

// gossip loops, sending deltas of the infostore and receiving deltas
// in turn. If an alternate is proposed on response, the client addr
// is modified and method returns for forwarding by caller.
func (c *client) gossip(g *Gossip) error {
	localMaxSeq := int64(0)
	remoteMaxSeq := int64(-1)
	for {
		// Do a periodic check to determine whether this outgoing client
		// is duplicating work already being done by an incoming client.
		// To avoid mutual shutdown, we only shutdown our client if our
		// server address is lexicographically less than the other.
		if g.hasIncoming(c.addr) && g.is.NodeAddr.String() < c.addr.String() {
			return util.Errorf("stopping outgoing client %s; already have incoming", c.addr)
		}

		// Compute the delta of local node's infostore to send with request.
		g.mu.Lock()
		delta := g.is.delta(c.addr, localMaxSeq)
		if delta != nil {
			localMaxSeq = delta.MaxSeq
		}
		g.mu.Unlock()

		// Send gossip with timeout.
		args := &GossipRequest{
			Addr:   g.is.NodeAddr,
			LAddr:  c.rpcClient.LAddr,
			MaxSeq: remoteMaxSeq,
			Delta:  delta,
		}
		reply := new(GossipResponse)
		gossipCall := c.rpcClient.Go("Gossip.Gossip", args, reply, nil)
		select {
		case <-gossipCall.Done:
			if gossipCall.Error != nil {
				return gossipCall.Error
			}
		case <-time.After(*gossipInterval * 2):
			// Allowed twice gossip interval.
			return util.Errorf("timeout after: %v", *gossipInterval*2)
		case <-c.closer:
			return nil
		}

		// Handle remote forwarding.
		if reply.Alternate != nil {
			log.Printf("received forward from %+v to %+v", c.addr, reply.Alternate)
			c.forwardAddr = reply.Alternate
			return nil
		}

		// Combine remote node's infostore delta with ours.
		now := time.Now().UnixNano()
		if reply.Delta != nil {
			g.mu.Lock()
			freshCount := g.is.combine(reply.Delta)
			if freshCount > 0 {
				c.lastFresh = now
			}
			remoteMaxSeq = reply.Delta.MaxSeq
			g.mu.Unlock()
		}
		// Check whether peer node is too boring--disconnect if yes.
		if (now - c.lastFresh) > int64(maxWaitForNewGossip) {
			return util.Errorf("peer is too boring")
		}
	}
}
