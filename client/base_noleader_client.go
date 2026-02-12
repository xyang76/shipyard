package client

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/state"
	"bufio"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

type NoLeaderClient struct {
	servers   []*BaseConn
	N         int
	success   int64
	failed    int64
	skipped   int64
	mu        sync.Mutex
	replyTime *ReplyTime
}

func NewNoLeaderClient(replicaAddrs []string, replyTime *ReplyTime) *NoLeaderClient {
	N := len(replicaAddrs)
	c := &NoLeaderClient{
		N:         N,
		servers:   make([]*BaseConn, N),
		replyTime: replyTime,
	}
	for i, addr := range replicaAddrs {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Printf("Failed to connect to replica %d: %v\n", i, err)
			continue
		}
		sc := &BaseConn{
			conn:   conn,
			reader: bufio.NewReader(conn),
			writer: bufio.NewWriter(conn),
		}
		c.servers[i] = sc
		go c.replyReader(int32(i), sc)
	}

	return c
}

// Non-blocking send to a replica
func (c *NoLeaderClient) NonBlockSend(replica int, args *genericsmrproto.Propose) {
	if replica < 0 || replica >= c.N || c.servers[replica] == nil {
		atomic.AddInt64(&c.skipped, 1)
		return
	}

	rc := c.servers[replica]
	if rc == nil {
		
	}

	err := genericsmr.WriteWithTimeout(rc.conn, func() error {
		if err := rc.writer.WriteByte(genericsmrproto.PROPOSE); err != nil {
			return err
		}
		args.Marshal(rc.writer)
		rc.writeCounter++
		if rc.writeCounter%100 == 0 {
			return rc.writer.Flush()
		}
		return nil
	})
	if err != nil {
		log.Printf("Replica %d failed, closing conn\n", replica)
		rc.conn.Close()
		c.servers[replica] = nil
		atomic.AddInt64(&c.failed, 1)
	}
}

// Flush all remaining Writes
func (c *NoLeaderClient) FlushAll() {
	for _, rc := range c.servers {
		if rc != nil {
			_ = rc.writer.Flush()
		}
	}
}

// Close all connections
func (c *NoLeaderClient) Close() {
	for _, rc := range c.servers {
		if rc != nil {
			rc.conn.Close()
		}
	}
}

func (c *NoLeaderClient) replyReader(sid int32, sc *BaseConn) {
	reply := new(genericsmrproto.ProposeReplyTS)

	for {
		err := genericsmr.ReadWithTimeout(sc.conn, func() error {
			return reply.Unmarshal(sc.reader)
		})

		if err != nil {
			// Timeout ≠ leader failure
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			c.CloseLeader(sid, sc)
			return
		}

		if reply.OK == config.TRUE {
			atomic.AddInt64(&c.success, 1)
		} else {
			if reply.CommandId != config.IdentifyLeader {
				atomic.AddInt64(&c.failed, 1)
			}
		}

		if c.replyTime != nil {
			c.replyTime.ReplyArrival(reply)
		}

		if reply.CommandId != config.IdentifyLeader &&
			reply.Value == state.NOTLeader &&
			reply.OK == config.FALSE {
			c.CloseLeader(sid, sc)
			return
		}
	}
}

func (c *NoLeaderClient) CloseLeader(sid int32, sc *BaseConn) {
	c.mu.Lock()
	dlog.Info("Leader %v fail, close the leader now!", sid)
	_ = sc.conn.Close()
	c.mu.Unlock()
}
