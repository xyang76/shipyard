package client

import (
	"Mix/config"
	"Mix/masterproto"
	"Mix/shard"
	"Mix/state"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func StartRecoveryShardClientSec() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	if *config.Conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	// --- 1. Connect to master ---
	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *config.MasterAddr, *config.MasterPort))
	if err != nil {
		log.Fatalf("Cannot connect to master: %v\n", err)
	}
	defer master.Close()

	// --- 2. Get replica list ---
	rl := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rl)
	if err != nil {
		log.Fatalf("GetReplicaList failed: %v\n", err)
	}

	if len(rl.ReplicaList) == 0 {
		log.Fatalf("No replicas returned by the master")
	}

	batch := *config.ReqsNum
	writePercent := *config.Writes
	turns := 100
	shards := shard.NewShardInfo()
	replyTime := NewReplyTime(turns, batch, shards)
	client := NewShardClient(rl.ReplicaList, shards, replyTime)

	// initial leaders
	for _, sid := range shards.Shards {
		client.findLeader(sid)
	}

	// success report
	last := int64(0)
	reqID := int32(0)
	ticker := time.NewTicker(1 * time.Second)
	start := time.Now()
	mu := sync.Mutex{}

	go func(reply *ReplyTime) {
		t := time.NewTicker(2 * time.Second)
		for range t.C {
			mu.Lock()
			fmt.Printf("Success so far: %d, this round %d, skipped:%v, send:%v\n",
				client.Success(), client.success-last, countTotal(reply.skipped), countTotal(reply.send))
			last = client.success
			mu.Unlock()
		}
	}(replyTime)

	for i := 0; i < turns; i++ {
		<-ticker.C
		for j := 0; j < batch; j++ {
			key := state.Key(int(reqID))
			sid, _ := client.shards.GetShardId(key)

			mu.Lock()
			arrived := atomic.LoadInt64(&replyTime.shardArrival[sid])
			if replyTime.send[sid]-arrived >= config.CHAN_BUFFER_SIZE/2 {
				replyTime.skipped[sid]++
				mu.Unlock()
			} else {
				replyTime.send[sid]++
				mu.Unlock()
				operation := client.RandomValue()
				if operation <= writePercent {
					client.NonBlockSend(reqID, key, state.PUT, state.Value(reqID))
				} else {
					client.NonBlockSend(reqID, key, state.GET, state.Value(reqID))
				}
				reqID++
			}
		}

		// flush all leaders
		client.mu.RLock()
		for _, l := range client.leaders {
			l.writer.Flush()
		}
		client.mu.RUnlock()
	}

	fmt.Printf("Finished. success=%d time=%v\n",
		client.Success(), time.Since(start))
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
