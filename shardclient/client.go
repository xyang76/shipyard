package shardclient

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/masterproto"
	"Mix/shard"
	"Mix/state"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type ClientManager struct {
	shards       *shard.ShardInfo
	clients      map[int32]*ShardClient
	replyTime    *ReplyTime
	rng          *rand.Rand
	master       *rpc.Client
	reqsPerRound int
	round        int
	conflicts    int
	writePercent int
}

func NewClientManager() *ClientManager {
	return &ClientManager{
		clients: make(map[int32]*ShardClient),
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (cm *ClientManager) Init() {
	dlog.Info("1")
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())
	// --- 1. Connect to master ---
	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *config.MasterAddr, *config.MasterPort))
	if err != nil {
		log.Fatalf("Cannot connect to master: %v\n", err)
	}
	cm.master = master

	dlog.Info("2")
	// --- 2. Get replica list ---
	rl := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rl)
	if err != nil {
		log.Fatalf("GetReplicaList failed: %v\n", err)
	}

	if len(rl.ReplicaList) == 0 {
		log.Fatalf("No replicas returned by the master")
	}

	dlog.Info("3")
	// -- 3. Init clients
	cm.shards = shard.NewShardInfo()
	cm.replyTime = NewReplyTime(100, 1000)
	for _, sid := range cm.shards.Shards {
		cm.clients[sid] = NewShardClient(rl.ReplicaList, sid, cm.shards, cm.replyTime)
	}
	dlog.Info("4")
}

func (cm *ClientManager) StartSingleTest() {
	reqsPerRound := cm.reqsPerRound
	round := cm.round
	conf := cm.conflicts
	writePercent := cm.writePercent
	replyTime := cm.replyTime

	before_total := time.Now()
	elapsed_sum := int64(0)
	reqID := int32(0)
	last := int64(0)

	go func(reply *ReplyTime) {
		t := time.NewTicker(1 * time.Second)
		for range t.C {
			fmt.Printf("Success so far: %d, this round %d, skipped:%v, send:%v\n",
				reply.success, reply.success-reply.lastRound, countTotal(reply.skipped), countTotal(reply.send))
			reply.lastRound = reply.success
		}
	}(replyTime)

	// steps for two loops
	for r := 0; r < round; r++ {
		wg := new(sync.WaitGroup)
		for _, l := range cm.clients {
			l.StartRound(wg)
			wg.Add(1)
		}

		startTime := time.Now()

		for i := 0; i < reqsPerRound; i++ {
			key := state.Key(int(reqID))
			//key := cm.shards.GetKey(l.shardID, reqID)
			sid, _ := cm.shards.GetShardId(key)
			if config.CurrentApproach == config.EPaxos || config.CurrentApproach == config.Mencius {
				conflict := cm.RandomValue()
				if conflict <= conf {
					key = state.Key(42)
				} else {
					key = state.Key(43 + reqID)
				}
			}
			operation := cm.RandomValue()
			if operation <= writePercent {
				cm.Send(sid, reqID, key, state.PUT, state.Value(reqID))
			} else {
				cm.Send(sid, reqID, key, state.GET, state.Value(reqID))
			}
			reqID++
		}

		for _, l := range cm.clients {
			l.Flush()
		}

		for _, l := range cm.clients {
			go func() {
				l.FinishRound()
			}()
		}

		wg.Wait()
		last = atomic.LoadInt64(&replyTime.replyTimes[r])
		if last == 0 {
			last = startTime.UnixNano()
		}
		elapsed := last - startTime.UnixNano()
		elapsed_sum += elapsed
		//fmt.Printf("Round %d finished: total success=%d of %d/%d, elapsed=%v\n", r, client.Success(), replyTime.roundArrivals[r], reqsPerRound, time.Duration(elapsed))
		if !config.PrintPerSec {
			fmt.Printf("Round %d finished: total success=%d(f:%d) of %d/%d, elapsed=%v\n",
				r, replyTime.success, replyTime.failed, replyTime.roundArr[r], reqsPerRound, time.Duration(elapsed))
		}
	}

	after_total := time.Now()
	fmt.Printf("Test took %v, sum %v\n", after_total.Sub(before_total), time.Duration(elapsed_sum))
}

func (cm *ClientManager) StartConcurrentTest() {
	reqsPerRound := cm.reqsPerRound
	round := cm.round
	conf := cm.conflicts
	writePercent := cm.writePercent
	rt := cm.replyTime

	go func(reply *ReplyTime) {
		t := time.NewTicker(1 * time.Second)
		for range t.C {
			fmt.Printf("Success so far: %d, this round %d, skipped:%v, send:%v\n",
				reply.success, reply.success-reply.lastRound, countTotal(reply.skipped), countTotal(reply.send))
			reply.lastRound = reply.success
		}
	}(rt)

	//before_total := time.Now()
	//elapsed_sum := int64(0)
	reqID := int32(0)
	total := 0
	mu := new(sync.Mutex)
	lastTime := int64(0)

	for _, l := range cm.clients {
		go func(l *ShardClient) {
			replyTime := l.replytime
			sid := l.shardID
			info := l.shardInfo
			for r := 0; r < round; r++ {
				wg := new(sync.WaitGroup)
				wg.Add(1)
				l.StartRound(wg)
				startTime := time.Now()

				for i := 0; i < reqsPerRound/len(cm.clients); i++ {
					key := info.GetKey(l.shardID, reqID)
					if config.CurrentApproach == config.EPaxos || config.CurrentApproach == config.Mencius {
						conflict := cm.RandomValue()
						if conflict <= conf {
							key = state.Key(42)
						} else {
							key = state.Key(43 + reqID)
						}
					}
					operation := cm.RandomValue()
					if operation <= writePercent {
						cm.Send(sid, reqID, key, state.PUT, state.Value(reqID))
					} else {
						cm.Send(sid, reqID, key, state.GET, state.Value(reqID))
					}
					reqID++
				}
				l.Flush()
				go func() {
					l.FinishRound()
				}()

				wg.Wait()
				lastTime = atomic.LoadInt64(&l.replytime.replyTimes[r])
				if lastTime == 0 {
					lastTime = startTime.UnixNano()
				}
				elapsed := lastTime - startTime.UnixNano()
				//fmt.Printf("Shard %v, Round %d finished: total success=%d(f:%d) of %d/%d, elapsed=%v\n", l.shardID,
				//	r, replyTime.success, replyTime.failed, replyTime.roundArr[r], reqsPerRound, time.Duration(elapsed))
				if !config.PrintPerSec {
					fmt.Printf("Round %d finished: total success=%d(f:%d) of %d/%d, elapsed=%v\n",
						r, replyTime.success, replyTime.failed, replyTime.roundArr[r], reqsPerRound, time.Duration(elapsed))
				}
				mu.Lock()
				total++
				mu.Unlock()
			}
		}(l)
	}
	for {
		mu.Lock()
		t := total
		mu.Unlock()
		if t == round*len(cm.clients) {
			break
		}
	}
	dlog.Info("Finished")
}

func (cm *ClientManager) Close() {
	for _, l := range cm.clients {
		l.Close()
	}
	cm.master.Close()
}

func (cm *ClientManager) FindLeaders() {
	for _, l := range cm.clients {
		l.findLeader()
	}
}

func (cm *ClientManager) Send(
	sid int32,
	reqID int32,
	key state.Key,
	op state.Operation,
	value state.Value,
) {
	c := cm.clients[sid]
	c.NonBlockSend(reqID, key, op, value)
}

func (cm *ClientManager) RandomValue() int {
	return cm.rng.Intn(101) // 0..100 inclusive
}

func countTotal(m map[int32]int64) string {
	str := "["
	total := int64(0)
	for k, v := range m {
		str += fmt.Sprintf("%d:%d,", k, v)
		total += v
	}
	str += fmt.Sprintf(", total = %d]", total)
	return str
}
