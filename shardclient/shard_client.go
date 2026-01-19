package shardclient

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/shard"
	"Mix/state"
	"bufio"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ShardConn struct {
	conn         net.Conn
	reader       *bufio.Reader
	writer       *bufio.Writer
	writeCounter int
}

type ShardClient struct {
	shardID   int32
	replicas  []string
	shardInfo *shard.ShardInfo

	mu        sync.RWMutex
	leader    *ShardConn
	searching bool

	sent    int64
	arrival int64
	success int64
	skipped int64
	failed  int64

	done      chan struct{}   // buffered channel for round completion
	waitGroup *sync.WaitGroup // external WaitGroup
	replytime *ReplyTime
}

func NewShardClient(replicas []string, shardId int32, shardInfo *shard.ShardInfo, replyTime *ReplyTime) *ShardClient {
	clientAddrs := replicas
	if *config.SeperateClientPort {
		clientAddrs = make([]string, len(replicas))
		for i, addr := range replicas {
			host, portStr, _ := net.SplitHostPort(addr)
			port, _ := strconv.Atoi(portStr)
			clientAddrs[i] = fmt.Sprintf("%s:%d", host, port+2000) // client port
		}
	}

	return &ShardClient{
		replicas:  clientAddrs,
		shardID:   shardId,
		replytime: replyTime,
		shardInfo: shardInfo,
	}
}

func (c *ShardClient) startLeaderDiscovery() {
	c.mu.Lock()
	if c.searching {
		c.mu.Unlock()
		return
	}
	c.searching = true
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			c.searching = false
			c.mu.Unlock()
		}()

		for {
			leader := c.findLeader()
			if leader != nil {
				c.mu.Lock()
				c.leader = leader
				c.mu.Unlock()
				return
			}

			// no leader yet → retry
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func (c *ShardClient) findLeader() *ShardConn {
	for _, addr := range c.replicas {
		conn, err := net.DialTimeout("tcp", addr, genericsmr.CONN_TIMEOUT)
		if err != nil {
			continue
		}

		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)
		sid := c.shardID

		args := genericsmrproto.Propose{
			CommandId: config.IdentifyLeader,
			Command: state.Command{
				Op: state.GET,
				K:  state.Key(sid),
			},
		}

		err = genericsmr.WriteWithTimeout(conn, func() error {
			if err := w.WriteByte(genericsmrproto.PROPOSE); err != nil {
				return err
			}
			args.Marshal(w)
			return w.Flush()
		})
		if err != nil {
			conn.Close()
			continue
		}

		reply := new(genericsmrproto.ProposeReplyTS)
		err = genericsmr.ReadWithTimeout(conn, func() error {
			return reply.Unmarshal(r)
		})
		if err != nil {
			conn.Close()
			continue
		}
		if reply.OK != 0 {
			fmt.Printf("Leader for shard %d found at %s\n", c.shardID, addr)

			sc := &ShardConn{
				conn:   conn,
				reader: r,
				writer: w,
			}

			c.mu.Lock()
			c.leader = sc
			c.mu.Unlock()
			go c.replyReader(sc)
			return sc
		}

		conn.Close()
	}

	return nil
}

func (c *ShardClient) replyReader(sc *ShardConn) {
	reply := new(genericsmrproto.ProposeReplyTS)

	for {
		err := genericsmr.ReadWithTimeout(sc.conn, func() error { return reply.Unmarshal(sc.reader) })
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			c.CloseLeader(sc)
			return
		}

		atomic.AddInt64(&c.arrival, 1)
		if reply.OK == config.TRUE {
			atomic.AddInt64(&c.success, 1)
		} else if reply.CommandId != config.IdentifyLeader {
			atomic.AddInt64(&c.failed, 1)
		}
		if c.replytime != nil {
			c.replytime.OnReplyArrival(reply)
		}

		if reply.CommandId != config.IdentifyLeader &&
			reply.Value == state.NOTLeader &&
			reply.OK == config.FALSE {
			c.CloseLeader(sc)
		}
		c.checkDone()
	}
}

func (c *ShardClient) Flush() {
	leader := c.leader
	if leader == nil {
		return
	}
	leader.writer.Flush()
}

func (c *ShardClient) NonBlockSend(reqID int32, key state.Key, op state.Operation, value state.Value) {
	c.mu.RLock()
	leader := c.leader
	searching := c.searching
	c.mu.RUnlock()

	atomic.AddInt64(&c.sent, 1)

	if leader == nil {
		if !searching {
			c.startLeaderDiscovery()
		}
		atomic.AddInt64(&c.skipped, 1)
		return
	}

	args := genericsmrproto.Propose{
		CommandId: reqID,
		Command: state.Command{
			Op: op,
			K:  key,
			V:  value,
		},
	}

	err := genericsmr.WriteWithTimeout(leader.conn, func() error {
		if err := leader.writer.WriteByte(genericsmrproto.PROPOSE); err != nil {
			return err
		}
		args.Marshal(leader.writer)
		leader.writeCounter++
		if leader.writeCounter%100 == 0 {
			return leader.writer.Flush()
		}
		return nil
	})

	if err != nil {
		atomic.AddInt64(&c.skipped, 1)
		c.CloseLeader(leader)
	}
}

func (c *ShardClient) CloseLeader(sc *ShardConn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dlog.Info("Leader of %v crashes, closed", c.shardID)
	c.updateDone()

	if c.leader == sc {
		c.leader = nil
		_ = sc.conn.Close()
	}
}

func (c *ShardClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.leader != nil {
		c.leader.conn.Close()
		c.leader = nil
	}
}

func (c *ShardClient) updateDone() {
	select {
	case c.done <- struct{}{}:
	default:
		// already sent
	}
}

func (c *ShardClient) checkDone() {
	//dlog.Info("sent %v, arrival %v, skipped %v", c.sent, c.arrival, c.skipped)
	if atomic.LoadInt64(&c.sent) <= atomic.LoadInt64(&c.arrival)+atomic.LoadInt64(&c.skipped) {
		c.updateDone()
	}
}

func (c *ShardClient) Reset() {
	atomic.StoreInt64(&c.sent, 0)
	atomic.StoreInt64(&c.arrival, 0)
	atomic.StoreInt64(&c.skipped, 0)
}

func (c *ShardClient) StartRound(wg *sync.WaitGroup) {
	c.waitGroup = wg
	c.done = make(chan struct{}, 1) // buffered so sending doesn't block
}

func (c *ShardClient) FinishRound() {
	c.checkDone()

	select {
	case <-c.done:
		// all replies received
	case <-time.After(time.Duration(*config.ReplyReceiveTimeout) * time.Millisecond):
		// timeout
	}
	if c.waitGroup != nil {
		c.waitGroup.Done()
	}
}

func (c *ShardClient) Success() int64 {
	return atomic.LoadInt64(&c.success)
}

func (c *ShardClient) Failed() int64 {
	return atomic.LoadInt64(&c.failed)
}
