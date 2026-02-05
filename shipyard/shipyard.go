package shipyard

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/fastrpc"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/shard"
	"Mix/state"
	"math/rand"
	"sync"
	"time"
)

type ClientRequests struct {
	Propose *genericsmr.Propose
	Index   int32
	Epoch   int32
	Apport  int32
}

type Replica struct {
	*genericsmr.Replica
	mu sync.Mutex

	shards        map[int32]*Skiff
	apportion     *Apportion
	shardInfo     *shard.ShardInfo
	peerIds       []int32
	leadingShards []int32
	shardLastTime map[int32]time.Time

	voteAndGatherChan      chan fastrpc.Serializable
	appendEntriesChan      chan fastrpc.Serializable
	voteAndGatherReplyChan chan fastrpc.Serializable
	appendEntriesReplyChan chan fastrpc.Serializable
	balanceChan            chan fastrpc.Serializable
	balanceReplyChan       chan fastrpc.Serializable
	heartbeatChan          chan fastrpc.Serializable
	heartbeatReplyChan     chan fastrpc.Serializable

	voteAndGatherRPC      uint8
	voteAndGatherReplyRPC uint8
	appendEntryRPC        uint8
	appendEntryReplyRPC   uint8
	balanceRPC            uint8
	balanceReplyRPC       uint8
	heartbeatRPC          uint8
	heartbeatReplyRPC     uint8

	leading       bool
	balancing     bool
	balanceTokens int
	leadingStopCh chan struct{}
	leadingStatus *LeadState
	Shutdown      bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {

	r := &Replica{
		Replica: genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),

		voteAndGatherChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		voteAndGatherReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		appendEntriesChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		appendEntriesReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		balanceChan:            make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		balanceReplyChan:       make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		heartbeatChan:          make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		heartbeatReplyChan:     make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),

		leading:       false,
		balancing:     false,
		Shutdown:      false,
		balanceTokens: 1,
	}

	if *config.ShardStatus {
		r.leadingStatus = NewLeadState(r)
		r.leadingStatus.StartTimer()
	}

	r.apportion = NewApportion(r)
	r.voteAndGatherRPC = r.RegisterRPC(new(VoteAndGatherArgs), r.voteAndGatherChan)
	r.voteAndGatherReplyRPC = r.RegisterRPC(new(VoteAndGatherReply), r.voteAndGatherReplyChan)
	r.appendEntryRPC = r.RegisterRPC(new(AppendEntriesArgs), r.appendEntriesChan)
	r.appendEntryReplyRPC = r.RegisterRPC(new(AppendEntriesReply), r.appendEntriesReplyChan)
	r.balanceRPC = r.RegisterRPC(new(BalanceArgs), r.balanceChan)
	r.balanceReplyRPC = r.RegisterRPC(new(BalanceReply), r.balanceReplyChan)
	r.heartbeatRPC = r.RegisterRPC(new(HeartbeatArgs), r.heartbeatChan)
	r.heartbeatReplyRPC = r.RegisterRPC(new(HeartbeatReply), r.heartbeatReplyChan)

	go r.run()
	return r
}

func (r *Replica) run() {
	r.ConnectToPeers()
	r.initPeers()
	r.initShards()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	tick := time.NewTicker(time.Duration(*config.TickTime*2) * time.Millisecond)
	defer tick.Stop()

	dlog.Println("Shipyard running, waiting for client proposes")

	for !r.Shutdown {
		select {
		case prop := <-r.ProposeChan:
			// got a client propose
			dlog.Printf("Shipyard got proposal Op=%d Id=%d\n", prop.Command.Op, prop.CommandId)
			r.handlePropose(prop)
			break

		case vgs := <-r.voteAndGatherChan:
			args := vgs.(*VoteAndGatherArgs)
			dlog.Printf("Received shard %d VoteAndGather from replica, epoch=%d apportion=%d commit=%d\n",
				args.Shard, args.Epoch, args.Apportion, args.CandidateCommit)

			if instance, ok := r.shards[args.Shard]; ok {
				instance.handleVoteAndGather(args)
			} else {
				dlog.Error("shard %d not exists in replica %d", args.Shard, r.Id)
			}
			break

		case vgrs := <-r.voteAndGatherReplyChan:
			reply := vgrs.(*VoteAndGatherReply)
			dlog.Printf("Received shard %d VoteAndGatherReply epoch=%d apportion=%d commit=%d OK=%v\n",
				reply.Shard, reply.Epoch, reply.Apportion, reply.PeerCommit, reply.OK)

			if instance, ok := r.shards[reply.Shard]; ok {
				instance.handleVoteAndGatherReply(reply)
			} else {
				dlog.Error("shard %d not exists in replica %d", reply.Shard, r.Id)
			}
			break
		case aes := <-r.appendEntriesChan:
			args := aes.(*AppendEntriesArgs)
			dlog.Printf("Received shard %d AppendEntries from leader %d epoch=%d apportion=%d commit=%d\n",
				args.Shard, args.LeaderId, args.Epoch, args.Apportion, args.LeaderCommit)

			if instance, ok := r.shards[args.Shard]; ok {
				instance.handleAppendEntries(args)
			} else {
				dlog.Error("shard %d not exists in replica %d", args.Shard, r.Id)
			}
			break
		case aers := <-r.appendEntriesReplyChan:
			reply := aers.(*AppendEntriesReply)
			dlog.Printf("Received shard %d AppendEntriesReply epoch=%d apportion=%d commit=%d OK=%v\n",
				reply.Shard, reply.Epoch, reply.Apportion, reply.CommitIndex, reply.OK)

			if instance, ok := r.shards[reply.Shard]; ok {
				instance.handleAppendEntriesReply(reply)
			} else {
				dlog.Error("shard %d not exists in replica %d", reply.Shard, r.Id)
			}
			break
		case bs := <-r.balanceChan:
			args := bs.(*BalanceArgs)
			dlog.Printf("Received shard %d Balance request from leader %d\n",
				args.Shard, args.LeaderId)

			if instance, ok := r.shards[args.Shard]; ok {
				instance.handleBalance(args)
			} else {
				dlog.Error("shard %d not exists in replica %d", args.Shard, r.Id)
			}
			break

		case brs := <-r.balanceReplyChan:
			reply := brs.(*BalanceReply)
			dlog.Printf("Received shard %d BalanceReply token=%v\n",
				reply.Shard, reply.Token)

			if instance, ok := r.shards[reply.Shard]; ok {
				instance.handleBalanceReply(reply)
			} else {
				dlog.Error("shard %d not exists in replica %d", reply.Shard, r.Id)
			}
			break
		case hbs := <-r.heartbeatChan:
			args := hbs.(*HeartbeatArgs)
			dlog.Printf("Received Heartbeat from leader %d\n")
			r.handleHeartbeat(args)
		}
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
			// directly reply failure to client
			preply := &genericsmrproto.ProposeReplyTS{
				OK:        config.FALSE,
				CommandId: propose.CommandId,
				Value:     state.NIL,
				Timestamp: 0,
			}
			r.ReplyProposeTS(preply, propose.Reply)
		}
	} else {
		dlog.Error("From the given key and mapping function, can not find its shard")
		preply := &genericsmrproto.ProposeReplyTS{
			OK:        config.FALSE,
			CommandId: propose.CommandId,
			Value:     state.NIL,
			Timestamp: 0,
		}
		r.ReplyProposeTS(preply, propose.Reply)
	}
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
	r.shards = make(map[int32]*Skiff, r.shardInfo.ShardNum)
	r.shardLastTime = make(map[int32]time.Time, r.shardInfo.ShardNum)
	for _, sid := range r.shardInfo.Shards {
		//dlog.Println("%d add shard %d", r.Id, sid)
		r.shards[sid] = NewSkiff(r, r.Id, r.peerIds, int32(sid))
		r.shardLastTime[sid] = time.Now()
	}

	go r.StartShardTimer()
}

func (r *Replica) StartShardTimer() {
	go func() {
		ticker := time.NewTicker(time.Duration(*config.HeartBeatTimeout) * time.Millisecond)
		defer ticker.Stop()

		// Infinite loop to check the alive code periodically
		for {
			select {
			case <-ticker.C:
				r.checkShardStatus()
			}
		}
	}()
}

func (r *Replica) StartLeadingTimer() {
	r.mu.Lock()
	defer r.mu.Unlock()
	dlog.Print("%v start leading timer with its shard %v", r.Id, r.leadingShards)
	if r.leadingStopCh != nil {
		// Timer already running
		return
	}

	r.leadingStopCh = make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Duration(*config.HeartBeatInterval) * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.sendHeartbeat()
			case <-r.leadingStopCh:
				return
			}
		}
	}()
}

func (r *Replica) StopLeadingTimer() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.leadingStopCh != nil {
		close(r.leadingStopCh)
		r.leadingStopCh = nil
	}
}

func (r *Replica) checkShardStatus() {
	currentTime := time.Now()
	r.mu.Lock()
	for shard, lastSeen := range r.shardLastTime {
		// Calculate the time since the last seen
		durationSinceLastSeen := currentTime.Sub(lastSeen).Milliseconds()
		// Check if the node has expired
		if int(durationSinceLastSeen) > *config.HeartBeatTimeout && !r.IsLeader(shard) {
			dlog.Info("rep:%v expired shard:%v", r.Id, shard)
			go func() {
				skiff := r.shards[shard]
				skiff.startElection(r.getCurrentApportion())
			}()
		}
	}
	r.mu.Unlock()
}

func (r *Replica) refreshTime(shard int32) {
	r.mu.Lock()
	r.shardLastTime[shard] = time.Now()
	r.mu.Unlock()
}

func (r *Replica) getCurrentApportion() int32 {
	return int32(encodeApportion(r.apportion.value()))
}

func (r *Replica) getCurrentLeaderSize() int {
	r.mu.Lock()
	size := len(r.leadingShards)
	r.mu.Unlock()
	//defer r.mu.Unlock()
	return size
}

func (r *Replica) changeRole(shard int32, role SkiffState) {
	if role == Leader {
		r.mu.Lock()
		r.leadingShards = addShard(r.leadingShards, shard)
		r.mu.Unlock()

		if !r.leading {
			r.leading = true
			r.StartLeadingTimer()
		}

	} else if role == Follower || role == Candidate {
		r.mu.Lock()
		r.leadingShards = removeShard(r.leadingShards, shard)
		size := len(r.leadingShards)
		r.mu.Unlock()

		if r.leading && size == 0 {
			r.leading = false
			r.StopLeadingTimer()
		}
	}
}

func (r *Replica) IsLeader(shard int32) bool {
	for _, v := range r.leadingShards {
		if v == shard {
			return true
		}
	}
	return false
}

func (r *Replica) sendHeartbeat() {
	ls := r.leadingShards
	msg := &HeartbeatArgs{
		Shards:    ls,
		Sender:    r.Id,
		Apportion: r.getCurrentApportion(),
	}
	for _, peer := range r.peerIds {
		r.SendMsg(peer, r.heartbeatRPC, msg)
	}
}

func (r *Replica) handleHeartbeat(args *HeartbeatArgs) {
	dlog.Printf("Replica: %v handling heartbeat (leader:%v, shards:%v)", r.Id, args.Sender, args.Shards)
	for _, shard := range args.Shards {
		r.refreshTime(shard)
	}
	shardId := r.randomShard()
	r.checkBalance(shardId, args.Sender, args.Apportion, r.shards[shardId])
}

func (r *Replica) randomShard() int32 {
	// pick random index
	n := rand.Intn(len(r.shards))

	i := 0
	for shardID := range r.shards {
		if i == n {
			return shardID
		}
		i++
	}
	return 0
}

func (r *Replica) needBalance(shard int32, apportion int32) bool {
	if r.apportion.Imbalance(int(apportion)) {
		return true
	}
	return false
}

func (r *Replica) checkBalance(shard int32, leaderId int32, apportion int32, skiff *Skiff) {
	if config.Auto_Balance && r.apportion.Imbalance(int(apportion)) && !r.balancing {
		r.balancing = true
		dlog.Info("rep:%v-shard:%v need balance {received leader:%v-app:%v vs cur:%v}", r.Id, shard, leaderId, decodeApportion(int(apportion)), skiff.currentApportion)
		time.AfterFunc(time.Duration(*config.BalanceRegenerate)*time.Millisecond, func() {
			r.balancing = false
		})
		go func() {
			skiff.startBalance(leaderId)
		}()
	}
}

func (r *Replica) allowBalance(s int32) bool {
	return true
}

func (r *Replica) grantToken(apportion int32, args *BalanceArgs) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.balanceTokens > 0 {
		r.balanceTokens--
		time.AfterFunc(time.Duration(*config.BalanceRegenerate)*time.Millisecond, func() {
			r.mu.Lock()
			r.balanceTokens++
			r.mu.Unlock()
		})
		return true
	}
	return false
}

// addShard appends shard if it's not already present
func addShard(shards []int32, shard int32) []int32 {
	for _, s := range shards {
		if s == shard {
			return shards // already present
		}
	}
	return append(shards, shard)
}

// removeShard removes all occurrences of the target shard
func removeShard(shards []int32, target int32) []int32 {
	result := shards[:0] // reuse underlying array
	for _, s := range shards {
		if s != target {
			result = append(result, s)
		}
	}
	return result
}
