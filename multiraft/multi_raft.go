package multiraft

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/fastrpc"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/raft"
	"Mix/shard"
	"Mix/state"
	"fmt"
	"runtime"
	"sync"
	"time"
)

type Replica struct {
	*genericsmr.Replica

	shards    map[int32]*ShardedRaft
	shardInfo *shard.ShardInfo
	peerIds   []int32

	requestVoteChan        chan fastrpc.Serializable
	appendEntriesChan      chan fastrpc.Serializable
	requestVoteReplyChan   chan fastrpc.Serializable
	appendEntriesReplyChan chan fastrpc.Serializable

	requestVoteRPC      uint8
	requestVoteReplyRPC uint8
	appendEntryRPC      uint8
	appendEntryReplyRPC uint8

	Shutdown bool
	mu       sync.Mutex
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
	r := &Replica{
		Replica:                genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		requestVoteChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		requestVoteReplyChan:   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		appendEntriesChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		appendEntriesReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),

		Shutdown: false,
	}

	r.requestVoteRPC = r.RegisterRPC(new(raft.RequestVoteArgs), r.requestVoteChan)
	r.requestVoteReplyRPC = r.RegisterRPC(new(raft.RequestVoteReply), r.requestVoteReplyChan)
	r.appendEntryRPC = r.RegisterRPC(new(raft.AppendEntriesArgs), r.appendEntriesChan)
	r.appendEntryReplyRPC = r.RegisterRPC(new(raft.AppendEntriesReply), r.appendEntriesReplyChan)
	go r.run()
	return r
}

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	//fmt.Printf("Alloc=%vMB TotalAlloc=%vMB Sys=%vMB NumGC=%v PauseTotalMs=%v\n",
	//	m.Alloc/1024/1024, m.TotalAlloc/1024/1024, m.Sys/1024/1024, m.NumGC, m.PauseTotalNs/1e6)

	fmt.Printf("HeapAlloc=%v MB HeapSys=%v MB NumGC=%v\n", m.HeapAlloc/1024/1024, m.HeapSys/1024/1024, m.NumGC)
}

func (r *Replica) run() {
	r.ConnectToPeers()
	r.initPeers()
	r.initShards()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	//tick := time.NewTicker(60 * time.Millisecond)
	tick := time.NewTicker(time.Duration(*config.TickTime*2) * time.Millisecond)
	defer tick.Stop()

	dlog.Println("MultiRaft replica running, waiting for client proposes")
	//counter := 0
	for !r.Shutdown {
		select {
		//case <-tick.C:
		//	counter++
		//	if counter%50 == 0 {
		//		printMemStats()
		//	}

		case prop := <-r.ProposeChan:
			// got a client propose
			dlog.Printf("MultiRaft replica got proposal Op=%d Id=%d\n", prop.Command.Op, prop.CommandId)
			r.handlePropose(prop)
			break

		case reqVoteS := <-r.requestVoteChan:
			reqVote := reqVoteS.(*raft.RequestVoteArgs)
			dlog.Printf("Received shard %d RequestVote from replica %d, with term %d\n", reqVote.Shard, reqVote.CandidateId, reqVote.Term)
			if instance, ok := r.shards[reqVote.Shard]; ok {
				instance.handleRequestVote(reqVote)
			} else {
				dlog.Error("shard %d not exists in replica %d", reqVote.Shard, r.Id)
			}
			break

		case reqVoteReplyS := <-r.requestVoteReplyChan:
			reqVoteReply := reqVoteReplyS.(*raft.RequestVoteReply)
			dlog.Printf("Received shard %d RequestVoteReply term %d, voteGranted=%v\n", reqVoteReply.Shard,
				reqVoteReply.Term, reqVoteReply.VoteGranted)
			if instance, ok := r.shards[reqVoteReply.Shard]; ok {
				instance.handleRequestVoteReply(reqVoteReply)
			} else {
				dlog.Error("shard %d not exists in replica %d", reqVoteReply.Shard, r.Id)
			}
			break

		case appendEntriesS := <-r.appendEntriesChan:
			appendEntries := appendEntriesS.(*raft.AppendEntriesArgs)
			dlog.Printf("Received shard %d AppendEntries from replica %d, term %d, leaderCommit=%d\n",
				appendEntries.Shard, appendEntries.LeaderId, appendEntries.Term, appendEntries.LeaderCommit)
			if instance, ok := r.shards[appendEntries.Shard]; ok {
				instance.handleAppendEntries(appendEntries)
			} else {
				dlog.Error("shard %d not exists in replica %d", appendEntries.Shard, r.Id)
			}
			break

		case appendEntriesReplyS := <-r.appendEntriesReplyChan:
			appendEntriesReply := appendEntriesReplyS.(*raft.AppendEntriesReply)
			dlog.Printf("Received shard %d AppendEntriesReply from replica %d, term %d, success=%v, LastLogIndex=%d\n",
				appendEntriesReply.Shard, appendEntriesReply.Term,
				appendEntriesReply.Success, appendEntriesReply.LastLogIndex)
			if instance, ok := r.shards[appendEntriesReply.Shard]; ok {
				instance.handleAppendEntriesReply(appendEntriesReply)
			} else {
				dlog.Error("shard %d not exists in replica %d", appendEntriesReply.Shard, r.Id)
			}
			break
		}

	}
}

func (r *Replica) getInstance(shard string) {

}

func (r *Replica) initPeers() {
	r.peerIds = make([]int32, 0, r.N-1)
	for i := int32(0); i < int32(r.N); i++ {
		if i == r.Id {
			continue
		}
		r.peerIds = append(r.peerIds, i)
	}
}

func (r *Replica) initShards() {
	r.shardInfo = shard.NewShardInfo()
	r.shards = make(map[int32]*ShardedRaft, r.shardInfo.ShardNum)
	for _, sid := range r.shardInfo.Shards {
		//dlog.Println("%d add shard %d", r.Id, sid)
		r.shards[sid] = NewShardedRaft(r, r.Id, r.peerIds, int32(sid))
	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	dlog.Print("%d received propose %v\n", r.Id, propose.Command)

	// Special command to identify the leader
	if propose.CommandId == config.IdentifyLeader {
		if sid, err := r.shardInfo.GetShardId(propose.Command.K); err == nil {
			if shard, ok := r.shards[sid]; ok && shard.role == Leader {
				preply := &genericsmrproto.ProposeReplyTS{config.TRUE, -1, state.ISLeader, 0}
				r.ReplyProposeTS(preply, propose.Reply)
				return
			}
		}

		preply := &genericsmrproto.ProposeReplyTS{config.FALSE, -1, state.NOTLeader, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	if sid, err := r.shardInfo.GetShardId(propose.Command.K); err == nil {
		ch := r.shards[sid].proposeChan
		select {
		case ch <- propose:
		default:
			dlog.Warn("replica %d: shard %d propose queue full", r.Id, sid)
		}
	} else {
		dlog.Error("From the given key and mapping function, can not find its shard")
	}
}
