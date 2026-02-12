package clientfail

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/shard"
	"Mix/state"
	"bufio"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type BaseClient struct {
	replicas []string
	shards   *shard.ShardInfo

	mu    sync.RWMutex
	conns map[int32]*BaseConn
	// only one leader search per shard
	searching map[int32]bool
	leaders   map[int32]int
	discovery bool
	rng       *rand.Rand

	success int64
	skipped int64
	failed  int64
	notify  *FinishNotify
}

func NewBaseClient(replicas []string) *BaseClient {
	clientAddrs := replicas
	if *config.SeperateClientPort {
		clientAddrs = make([]string, len(replicas))
		for i, addr := range replicas {
			host, portStr, _ := net.SplitHostPort(addr)
			port, _ := strconv.Atoi(portStr)
			clientAddrs[i] = fmt.Sprintf("%s:%d", host, port+2000) // client port
		}
	}
	return &BaseClient{
		replicas:  clientAddrs,
		conns:     make(map[int32]*BaseConn),
		shards:    shard.NewShardInfo(),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		discovery: config.NeedLeaderDiscovery(),
		leaders:   make(map[int32]int),
	}
}

func (c *BaseClient) ConnectAll() {
	reqsPerRound := *config.ReqsNum
	round := *config.Rounds
	notify := NewFinishNotify(round, reqsPerRound, c.shards)
	c.notify = notify
	for i := 0; i < len(c.replicas); i++ {
		conn := NewBaseConn(c.replicas[i], i, notify)
		conn.InitConnection()
		c.conns[int32(i)] = conn
	}
}

func (c *BaseClient) findLeader(sid int32) int {
	for index, addr := range c.replicas {
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

			c.mu.Lock()
			c.leaders[sid] = index
			c.mu.Unlock()

			return index
		}
		fmt.Printf("Leader for shard %d not found at %s\n", sid, addr)
		conn.Close()
	}

	return -1
}

func (c *BaseClient) StartTests() {
	dlog.Info("starting client tests now ... ")
	before_total := time.Now()
	elapsed_sum := int64(0)
	reqsPerRound := *config.ReqsNum
	writePercent := *config.Writes
	round := *config.Rounds
	reqID := int32(0)
	notify := c.notify
	// steps for two loops
	for r := 0; r < round; r++ {
		startTime := time.Now()

		for i := 0; i < reqsPerRound; i++ {
			key := state.Key(int(reqID))
			connId := int32(c.getConn(reqID, key))
			if connId == -1 {
				c.skipped++
				notify.notifyCommandSkip(reqID)
				continue
			}
			conn := c.conns[connId]

			operation := c.RandomValue()
			var args genericsmrproto.Propose
			if operation <= writePercent {
				args = genericsmrproto.Propose{
					CommandId: reqID,
					Command: state.Command{
						Op: state.PUT,
						K:  key,
						V:  state.Value(reqID),
					},
				}
			} else {
				args = genericsmrproto.Propose{
					CommandId: reqID,
					Command: state.Command{
						Op: state.GET,
						K:  key,
						V:  state.Value(reqID),
					},
				}
			}
			conn.SendPropose(&args)
			reqID++
		}
		// wait for all replies or timeout(leader crash/lost connection etc)
		select {
		case <-notify.done[r]:
			// all replies received
		case <-time.After(time.Duration(*config.ReplyReceiveTimeout) * time.Millisecond):
			// timeout
		}

		last := atomic.LoadInt64(&notify.replyTimes[r])
		if last == 0 {
			last = startTime.UnixNano()
		}
		elapsed := last - startTime.UnixNano()
		elapsed_sum += elapsed
		//fmt.Printf("Round %d finished: total success=%d of %d/%d, elapsed=%v\n", r, client.Success(), replyTime.roundArrivals[r], reqsPerRound, time.Duration(elapsed))
		if !config.PrintPerSec {
			fmt.Printf("Round %d finished: total success=%d(s:%d-f:%d) of %d/%d, elapsed=%v\n",
				r, notify.success, notify.skipped, notify.failed, notify.roundArr[r], reqsPerRound, time.Duration(elapsed))
		}
	}
	after_total := time.Now()
	fmt.Printf("ClientWithFail took %v, sum %v\n", after_total.Sub(before_total), time.Duration(elapsed_sum))
}

func (c *BaseClient) getConn(reqId int32, key state.Key) int {
	if c.discovery {
		sid, _ := c.shards.GetShardId(key)
		c.mu.Lock()
		leader, exists := c.leaders[sid]
		c.mu.Unlock()
		if !exists {
			return c.findLeader(sid)
		}
		return leader
	} else {
		return int(reqId) % c.shards.ShardNum
	}
	return 0
}

func (c *BaseClient) PrintInfo() {
	//total := 0
	//for {
	//	//fmt.Printf("replicas total %v\n", total)
	//	c.notify.printInfo()
	//	time.Sleep(time.Second)
	//	total++
	//}
}

func (c *BaseClient) RandomValue() int {
	return c.rng.Intn(101) // 0..100 inclusive
}
