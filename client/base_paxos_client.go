package client

import (
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"sync/atomic"
)

var reqsNum *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var writes *int = flag.Int("w", 70, "Percentage of updates (writes). Defaults to 100%.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var fast *bool = flag.Bool("f", false, "Fast Paxos: send message directly to all replicas. Defaults to false.")
var rounds *int = flag.Int("rnd", 10, "Split the total number of requests into this many rounds, and do rounds sequentially. Defaults to 1.")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var eps *int = flag.Int("eps", 0, "Send eps more messages per round than the client will wait for (to discount stragglers). Defaults to 0.")
var conflicts *int = flag.Int("c", 100, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("zs", 2, "Zipfian s parameter")
var v = flag.Float64("zv", 1, "Zipfian v parameter")

var N int
var successful []int
var rarray []int
var rsp []bool

func waitReplies(readers []*bufio.Reader, leader int, n int, done chan bool) {
	e := false

	reply := new(genericsmrproto.ProposeReplyTS)
	for i := 0; i < n; i++ {
		if err := reply.Unmarshal(readers[leader]); err != nil {
			fmt.Println("Error when reading:", err)
			e = true
			continue
		}
		//fmt.Println(reply.Value)
		if *check {
			if rsp[reply.CommandId] {
				fmt.Println("Duplicate reply", reply.CommandId)
			}
			rsp[reply.CommandId] = true
		}
		if reply.OK != 0 {
			successful[leader]++
		}
	}
	done <- e
}

type ReplicaConn struct {
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	writeCounter int
}

type PaxosClient struct {
	servers   []*ReplicaConn
	N         int
	noLeader  bool
	success   int64
	failed    int64
	skipped   int64
	leaders   map[int32]*ReplicaConn
	searching map[int32]bool
	replyTime *ReplyTime
}

func NewPaxosClient(replicaAddrs []string, replyTime *ReplyTime, noLeader bool) *PaxosClient {
	N := len(replicaAddrs)
	servers := make([]*ReplicaConn, N)
	for i, addr := range replicaAddrs {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Printf("Failed to connect to replica %d: %v\n", i, err)
			continue
		}
		servers[i] = &ReplicaConn{
			conn:   conn,
			reader: bufio.NewReader(conn),
			writer: bufio.NewWriter(conn),
		}
	}

	return &PaxosClient{
		servers:   servers,
		N:         N,
		noLeader:  noLeader,
		leaders:   make(map[int32]*ReplicaConn),
		searching: make(map[int32]bool),
		replyTime: replyTime,
	}
}

// Non-blocking send to a replica
func (c *PaxosClient) NonBlockSend(replica int, args *genericsmrproto.Propose) {
	if replica < 0 || replica >= c.N || c.servers[replica] == nil {
		atomic.AddInt64(&c.skipped, 1)
		return
	}

	rc := c.servers[replica]

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

// Flush all remaining writes
func (c *PaxosClient) FlushAll() {
	for _, rc := range c.servers {
		if rc != nil {
			_ = rc.writer.Flush()
		}
	}
}

// Close all connections
func (c *PaxosClient) Close() {
	for _, rc := range c.servers {
		if rc != nil {
			rc.conn.Close()
		}
	}
}
