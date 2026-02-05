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

func StartRecoveryShardClient() {
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

	// initial parameters
	shards := shard.NewShardInfo()
	reqsPerRound := *ReqsNum
	//round := *Rounds
	round := *Rounds
	writePercent := *Writes
	reqID := int32(0)
	last := int64(0)
	replyTime := NewReplyTime(round, reqsPerRound, shards)
	client := NewShardClient(rl.ReplicaList, shards, replyTime)

	if config.PrintPerSec {
		go func(reply *ReplyTime) {
			t := time.NewTicker(1 * time.Second)
			for range t.C {
				fmt.Printf("Success so far: %d, this round %d, skipped:%v, send:%v\n",
					client.Success(), client.success-last, countTotal(reply.skipped), countTotal(reply.send))
				last = client.success
			}
		}(replyTime)
	}

	// initial leaders
	for _, sid := range shards.Shards {
		client.findLeader(sid)
	}

	before_total := time.Now()
	elapsed_sum := int64(0)
	// steps for two loops
	for r := 0; r < round; r++ {
		startTime := time.Now()

		for i := 0; i < reqsPerRound; i++ {
			key := state.Key(int(reqID))
			if config.CurrentApproach == config.EPaxos || config.CurrentApproach == config.Mencius {
				conflict := client.RandomValue()
				if conflict <= *conflicts {
					key = state.Key(42)
				} else {
					key = state.Key(43 + reqID)
				}
			}
			if *config.Balanced == 1 {
				key = shards.GetUnbalancedKey(reqID)
			}
			operation := client.RandomValue()
			if operation <= writePercent {
				client.NonBlockSend(reqID, key, state.PUT, state.Value(reqID))
			} else {
				client.NonBlockSend(reqID, key, state.GET, state.Value(reqID))
			}
			reqID++
		}

		for _, l := range client.leaders {
			l.writer.Flush()
		}

		// wait for all replies or timeout(leader crash/lost connection etc)
		select {
		case <-replyTime.done[r]:
			// all replies received
		case <-time.After(time.Duration(*config.ReplyReceiveTimeout) * time.Millisecond):
			// timeout
		}

		last := atomic.LoadInt64(&replyTime.replyTimes[r])
		if last == 0 {
			last = startTime.UnixNano()
		}
		elapsed := last - startTime.UnixNano()
		elapsed_sum += elapsed
		//fmt.Printf("Round %d finished: total success=%d of %d/%d, elapsed=%v\n", r, client.Success(), replyTime.roundArrivals[r], reqsPerRound, time.Duration(elapsed))
		if !config.PrintPerSec {
			fmt.Printf("Round %d finished: total success=%d(s:%d-f:%d) of %d/%d, elapsed=%v\n",
				r, client.Success(), client.skipped, client.failed, replyTime.roundArrivals[r], reqsPerRound, time.Duration(elapsed))
		}
	}

	after_total := time.Now()
	fmt.Printf("ClientWithFail took %v, sum %v\n", after_total.Sub(before_total), time.Duration(elapsed_sum))

	// --- after all Rounds ---
	client.Close()
	master.Close()
}
