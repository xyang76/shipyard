package client

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/masterproto"
	"Mix/shard"
	"Mix/state"
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ReplyTime struct {
	round         int
	numReqs       int
	replyTimes    []int64
	shardArrival  []int64
	roundArrivals []int64
	skipped       map[int32]int64
	send          map[int32]int64
	done          []chan bool
	shards        *shard.ShardInfo
}

func NewReplyTime(round int, numReqs int, shards *shard.ShardInfo) *ReplyTime {
	done := make([]chan bool, round)
	for i := 0; i < round; i++ {
		done[i] = make(chan bool, 1) // buffered, so sending doesn't block
	}
	return &ReplyTime{
		round:         round,
		numReqs:       numReqs,
		replyTimes:    make([]int64, round),
		shardArrival:  make([]int64, shards.ShardNum),
		roundArrivals: make([]int64, round),
		skipped:       make(map[int32]int64),
		send:          make(map[int32]int64),
		done:          done,
		shards:        shards,
	}
}

func (rt *ReplyTime) OnShardLeaderReset(sid int32) {
	atomic.StoreInt64(&rt.shardArrival[sid], 0)
	rt.send[sid] = 0
}

func (rt *ReplyTime) ReplyArrival(reply *genericsmrproto.ProposeReplyTS) {
	r := int(reply.CommandId) / rt.numReqs
	key := state.Key(reply.CommandId)
	sid, _ := rt.shards.GetShardId(key)
	atomic.StoreInt64(&rt.replyTimes[r], time.Now().UnixNano())
	arrivals := atomic.AddInt64(&rt.roundArrivals[r], 1)
	atomic.AddInt64(&rt.shardArrival[sid], 1)
	if arrivals == int64(rt.numReqs) {
		select {
		case rt.done[r] <- true:
		default:
			// already signaled
		}
	}
}

func (rt *ReplyTime) ReplySkip(reqID int32) {
	r := int(reqID) / rt.numReqs
	key := state.Key(reqID)
	sid, _ := rt.shards.GetShardId(key)
	atomic.StoreInt64(&rt.replyTimes[r], time.Now().UnixNano())
	arrivals := atomic.AddInt64(&rt.roundArrivals[r], 1)
	atomic.AddInt64(&rt.shardArrival[sid], 1)
	time.Sleep(1 * time.Second)
	if arrivals == int64(rt.numReqs) {
		select {
		case rt.done[r] <- true:
		default:
			// already signaled
		}
	}
}

type ShardClient struct {
	replicas []string
	shards   *shard.ShardInfo

	mu      sync.RWMutex
	leaders map[int32]*BaseConn
	// only one leader search per shard
	searching map[int32]bool
	rng       *rand.Rand

	success int64
	skipped int64
	failed  int64

	replyTime *ReplyTime
}

func NewShardClient(replicas []string, shards *shard.ShardInfo, replyTime *ReplyTime) *ShardClient {
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
		shards:    shards,
		leaders:   make(map[int32]*BaseConn),
		searching: make(map[int32]bool),
		replyTime: replyTime,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (c *ShardClient) Success() int64 {
	return atomic.LoadInt64(&c.success)
}

func (c *ShardClient) Failed() int64 {
	return atomic.LoadInt64(&c.failed)
}

func (c *ShardClient) Skipped() int64 {
	return atomic.LoadInt64(&c.skipped)
}

func (c *ShardClient) findPaxosLeader(master *rpc.Client) *BaseConn {
	reply := new(masterproto.GetLeaderReply)
	if err := master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
		log.Fatalf("Error making the GetLeader RPC\n")
	}
	leader := reply.LeaderId
	log.Printf("The leader is replica %d\n", leader)
	return nil
}

func (c *ShardClient) findLeader(sid int32) *BaseConn {
	for _, addr := range c.replicas {
		dlog.Info("dial replica %v", addr)
		conn, err := net.DialTimeout("tcp", addr, genericsmr.CONN_TIMEOUT)
		if err != nil {
			continue
		}

		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)

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
			fmt.Printf("Leader for shard %d found at %s\n", sid, addr)

			sc := &BaseConn{
				conn:   conn,
				reader: r,
				writer: w,
			}

			c.mu.Lock()
			c.leaders[sid] = sc
			c.replyTime.OnShardLeaderReset(sid)
			c.mu.Unlock()

			go c.replyReader(sid, sc)
			return sc
		}
		fmt.Printf("Leader for shard %d not found at %s\n", sid, addr)
		conn.Close()
	}

	return nil
}

func (c *ShardClient) findLeaderAsync(sid int32, finishChan chan struct{}) {
	var wg sync.WaitGroup

	for _, addr := range c.replicas {
		addr := addr // capture loop variable
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, genericsmr.CONN_TIMEOUT)
			if err != nil {
				return
			}
			r := bufio.NewReader(conn)
			w := bufio.NewWriter(conn)

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
				return
			}

			reply := new(genericsmrproto.ProposeReplyTS)
			err = genericsmr.ReadWithTimeout(conn, func() error {
				return reply.Unmarshal(r)
			})
			if err != nil {
				conn.Close()
				return
			}

			if reply.OK != 0 {
				dlog.Info("Searching find leader %v ...", addr)
				sc := &BaseConn{
					conn:   conn,
					reader: r,
					writer: w,
				}

				// ---- install leader if none exists ----
				c.mu.Lock()
				if _, exists := c.leaders[sid]; !exists {
					c.leaders[sid] = sc
					c.replyTime.OnShardLeaderReset(sid)
					c.mu.Unlock()

					fmt.Printf("Leader for shard %d found at %s\n", sid, addr)
					go c.replyReader(sid, sc)

					return
				}
				c.mu.Unlock()
			} else {
				dlog.Info("Searching not find leader %v ...", addr)
			}

			conn.Close()
		}()
	}

	// Notify when all goroutines are done
	go func() {
		wg.Wait()
		close(finishChan)
	}()
}

func (c *ShardClient) replyReader(sid int32, sc *BaseConn) {
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

func (c *ShardClient) CloseLeader(sid int32, sc *BaseConn) {
	c.mu.Lock()
	dlog.Info("Leader %v fail, close the leader now!", sid)
	if cur, ok := c.leaders[sid]; ok && cur == sc {
		delete(c.leaders, sid)
		_ = sc.conn.Close()
	}
	c.mu.Unlock()
}

func (c *ShardClient) FlushAll() {
	c.mu.RLock()
	leaders := c.leaders
	c.mu.RUnlock()

	for _, l := range leaders {
		_ = genericsmr.WriteWithTimeout(
			l.conn,
			func() error {
				return l.writer.Flush()
			},
		)
	}
}

//if leader == nil {
//	// Start leader discovery only if not already searching
//	if !searching {
//		c.mu.Lock()
//		c.searching[sid] = true
//		c.mu.Unlock()
//
//		go func() {
//			l := c.findLeader(sid)
//			c.mu.Lock()
//			c.searching[sid] = false
//			if l == nil {
//				delete(c.leaders, sid)
//			}
//			c.mu.Unlock()
//		}()
//	}
//
//	atomic.AddInt64(&c.skipped, 1)
//	return
//}

func (c *ShardClient) NonBlockSend(reqID int32, key state.Key, op state.Operation, value state.Value) {
	sid, _ := c.shards.GetShardId(key)

	c.mu.RLock()
	leader := c.leaders[sid]
	searching := c.searching[sid]
	c.mu.RUnlock()

	if leader == nil {
		// Start leader discovery only if not already searching
		if !searching {
			c.mu.Lock()
			c.searching[sid] = true
			c.mu.Unlock()
			dlog.Info("Start leader searching ... ")
			finishChan := make(chan struct{})

			go func() {
				c.findLeaderAsync(sid, finishChan)

				// Wait for discovery to finish
				<-finishChan
				c.mu.Lock()
				c.searching[sid] = false

				// If still no leader after probing, clean up
				if _, ok := c.leaders[sid]; !ok {
					dlog.Info("Still no leader found ... ")
					delete(c.leaders, sid)
				}

				c.mu.Unlock()
			}()
		}

		atomic.AddInt64(&c.skipped, 1)
		c.replyTime.ReplySkip(reqID)
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

	err := genericsmr.WriteWithTimeout(
		leader.conn,
		func() error {
			if err := leader.writer.WriteByte(genericsmrproto.PROPOSE); err != nil {
				return err
			}

			args.Marshal(leader.writer)
			leader.writeCounter++

			if leader.writeCounter%100 == 0 {
				if err := leader.writer.Flush(); err != nil {
					return err
				}
			}
			return nil
		},
	)

	if err != nil {
		c.CloseLeader(sid, leader)
	}
}

func (c *ShardClient) BlockSend(reqID int32, key state.Key, op state.Operation, value state.Value) {
	sid, _ := c.shards.GetShardId(key)

	c.mu.RLock()
	leader := c.leaders[sid]
	c.mu.RUnlock()

	if leader == nil {
		leader = c.findLeader(sid)
		if leader == nil {
			atomic.AddInt64(&c.skipped, 1)
			return
		}
	}

	args := genericsmrproto.Propose{
		CommandId: reqID,
		Command: state.Command{
			Op: op,
			K:  key,
			V:  value,
		},
	}

	err := genericsmr.WriteWithTimeout(
		leader.conn,
		func() error {
			if err := leader.writer.WriteByte(genericsmrproto.PROPOSE); err != nil {
				return err
			}

			args.Marshal(leader.writer)
			leader.writeCounter++

			if leader.writeCounter%100 == 0 {
				if err := leader.writer.Flush(); err != nil {
					return err
				}
			}
			return nil
		},
	)

	if err != nil {
		c.CloseLeader(sid, leader)
	}
}

func (c *ShardClient) RandomValue() int {
	return c.rng.Intn(101) // 0..100 inclusive
}

func (c *ShardClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for sid, sc := range c.leaders {
		if sc != nil {
			_ = sc.conn.Close()
		}
		delete(c.leaders, sid)
	}
}
