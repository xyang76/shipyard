package client

import (
	"Mix/config"
	"Mix/genericsmrproto"
	"Mix/masterproto"
	"Mix/shard"
	"Mix/state"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"runtime"
	"sync/atomic"
	"time"
)

func StartPaxosClient2() {
	flag.Parse()
	runtime.GOMAXPROCS(*config.Procs)

	// --- random key generation ---
	randObj := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(randObj, *s, *v, uint64(*reqsNum / *rounds + *eps))
	if *conflicts > 100 {
		log.Fatalf("Conflicts percentage must be 0..100")
	}

	// --- connect to master ---
	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *config.MasterAddr, *config.MasterPort))
	if err != nil {
		log.Fatalf("Cannot connect to master: %v\n", err)
	}
	defer master.Close()

	// --- get replica list ---
	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("GetReplicaList failed: %v\n", err)
	}

	shards := shard.NewShardInfo()
	replyTime := NewReplyTime(*rounds, *reqsNum / *rounds, shards)
	client := NewPaxosClient(rlReply.ReplicaList, replyTime, *noLeader)

	// --- generate requests ---
	rarray := make([]int, *reqsNum / *rounds + *eps)
	karray := make([]int64, len(rarray))
	put := make([]bool, len(rarray))
	for i := 0; i < len(rarray); i++ {
		r := rand.Intn(client.N)
		rarray[i] = r
		if *conflicts >= 0 {
			if rand.Intn(100) < *conflicts {
				karray[i] = 42
			} else {
				karray[i] = int64(43 + i)
			}
			put[i] = rand.Intn(100) < *writes
		} else {
			karray[i] = int64(zipf.Uint64())
		}
	}

	// --- leader election ---
	leader := 0
	if !*noLeader {
		reply := new(masterproto.GetLeaderReply)
		master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply)
		leader = reply.LeaderId
		log.Printf("Leader is replica %d\n", leader)
	}
	before_total := time.Now()
	elapsed_sum := int64(0)
	reqID := int32(0)
	for round := 0; round < *rounds; round++ {
		n := *reqsNum / *rounds
		startTime := time.Now()

		for i := 0; i < n+*eps; i++ {
			args := genericsmrproto.Propose{
				CommandId: reqID,
				Command: state.Command{
					Op: state.GET,
					K:  state.Key(karray[i]),
					V:  state.Value(reqID),
				},
			}
			if put[i] {
				args.Command.Op = state.PUT
			}

			if !*fast {
				target := leader
				if *noLeader {
					target = rarray[i]
				}
				client.NonBlockSend(target, &args)
			} else {
				for rep := 0; rep < client.N; rep++ {
					client.NonBlockSend(rep, &args)
				}
			}
			reqID++
		}

		// flush all writes at end of round
		client.FlushAll()

		// wait for replies or timeout
		select {
		case <-replyTime.done[round]:
		case <-time.After(time.Duration(*config.ReplyReceiveTimeout) * time.Millisecond):
		}

		last := atomic.LoadInt64(&replyTime.replyTimes[round])
		if last == 0 {
			last = startTime.UnixNano()
		}
		elapsed := last - startTime.UnixNano()
		elapsed_sum += elapsed
		fmt.Printf("Round %d finished: total success=%d of %d/%d, elapsed=%v\n",
			round, client.success, replyTime.roundArrivals[round], n, time.Duration(elapsed))
	}

	after_total := time.Now()
	fmt.Printf("Test took %v, sum %v\n", after_total.Sub(before_total), time.Duration(elapsed_sum))
	client.Close()
	master.Close()
}
