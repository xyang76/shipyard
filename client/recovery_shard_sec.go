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
	"sync/atomic"
	"time"
)

func StartRecoveryShardClientSec() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	if *conflicts > 100 {
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

	batch := *reqsNum
	turns := 100
	shards := shard.NewShardInfo()
	replyTime := NewReplyTime(turns, batch)
	client := NewShardClient(rl.ReplicaList, shards, replyTime)

	// initial leaders
	for _, sid := range shards.Shards {
		client.findLeader(sid)
	}

	// success report
	go func() {
		t := time.NewTicker(2 * time.Second)
		for range t.C {
			fmt.Printf("Success so far: %d\n", client.Success())
		}
	}()

	reqID := int32(0)

	ticker := time.NewTicker(1 * time.Second)
	start := time.Now()
	skipped := make(map[int32]int32)

	for i := 0; i < turns; i++ {
		<-ticker.C
		for j := 0; j < batch; j++ {
			key := state.Key(int(reqID))
			sid, _ := client.shards.GetShardId(key)

			lastId := atomic.LoadInt32(&replyTime.replyIds[sid])

			if reqID-lastId-skipped[sid] > config.CHAN_BUFFER_SIZE/2 {
				skipped[sid]++
			} else {
				client.NonBlockSend(reqID, key, state.PUT, state.Value(reqID))
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
