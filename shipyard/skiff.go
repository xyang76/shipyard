package shipyard

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/state"
	"fmt"
	"sort"
	"sync"
	"time"
)

const MAX_VOTE_LOG_SIZE = 200

const Empty = -1

type SkiffState int

const (
	Null SkiffState = iota
	Follower
	Candidate
	Leader
	Dead
)

type Skiff struct {
	replica *Replica
	role    SkiffState
	Id      int32
	peerIds []int32
	shard   int32
	mu      sync.Mutex

	// pre-allocated log
	//log             []LogEntry
	//logSize         int32 // tracks the number of valid entries
	log             *TruncatedLog
	proposeChan     chan *genericsmr.Propose
	pendingRequests map[int32]*ClientRequests
	pendingReads    []*ClientRequests

	commitIndex      int32
	lastApplied      int32
	votedFor         int32
	voteReceived     int
	currentEpoch     int32
	currentApportion int32
	lastCommmitIndex int32 //Since here is not RPC, we store it to record the latest commitIndex
	grantedApportion int32

	//Since here is not RPC, we store it to record the token and balance received
	tokenAcquired   bool
	balanceReceived int

	nextIndex     map[int32]int32 //Optimization for avoid redundent broadcast
	peerIndex     map[int32]int32
	logAppendChan chan struct{}

	electionResetEvent time.Time
	electionLastTerm   int32
}

func NewSkiff(repl *Replica, id int32, peerIds []int32, shard int32) *Skiff {
	r := &Skiff{
		replica: repl,
		Id:      id,
		peerIds: peerIds,
		shard:   shard,
		//log:             make([]LogEntry, 15*1024*1024), // preallocate 15M entries
		//logSize:         0,                              // initially empty
		log:             NewTruncatedLog(config.LOG_SIZE),
		logAppendChan:   make(chan struct{}, config.CHAN_BUFFER_SIZE),
		proposeChan:     make(chan *genericsmr.Propose, config.CHAN_BUFFER_SIZE),
		pendingRequests: make(map[int32]*ClientRequests),
		commitIndex:     Empty,
		nextIndex:       make(map[int32]int32),
		peerIndex:       make(map[int32]int32),
		role:            Candidate,
		votedFor:        -1,
		lastApplied:     -1,
	}

	if config.Read_Local {
		r.pendingReads = make([]*ClientRequests, 0, config.LOG_SIZE)
	}

	// Initialize peer's latest commit as -2 (Unknown) for smaller optimization.
	// This could be -1 as well
	for _, peerId := range r.peerIds {
		r.nextIndex[peerId] = -1
		r.peerIndex[peerId] = -1
	}

	go r.run()
	return r
}

func (r *Skiff) run() {
	if r.replica.Exec {
		go r.executeCommands()
	}

	onOffProposeChan := r.proposeChan

	tick := time.NewTicker(time.Duration(*config.TickTime) * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			onOffProposeChan = r.proposeChan
			break
		case prop := <-onOffProposeChan:
			if config.Read_Local {
				r.handleRWPropose(prop)
			} else {
				r.handlePropose(prop)
			}
			onOffProposeChan = nil
			break
		}
	}
}

// If not execute commands, server will directly reply based on commitLog function
func (r *Skiff) commitLog(from int32, to int32) {
	dlog.Print("%v commiting log from %d to %d, with log size %d", r.getShardInfo(), from, to, r.log.Size())

	for i := from + 1; i <= to; i++ {
		r.mu.Lock()
		req := r.pendingRequests[i]
		r.mu.Unlock()
		if req != nil && !r.replica.Dreply {
			entry := r.log.Get(i)
			if req.Epoch == entry.Epoch && req.Apport == entry.Apportion && req.Index == i {
				// reply to client that their request was accepted (TRUE)
				propreply := &genericsmrproto.ProposeReplyTS{
					config.TRUE,
					req.Propose.CommandId,
					state.NIL,
					req.Propose.Timestamp,
				}
				r.replica.ReplyProposeTS(propreply, req.Propose.Reply)
			} else {
				// reply to client that their request was not accepted (FALSE), maybe term mismatch due to another leader
				propreply := &genericsmrproto.ProposeReplyTS{
					config.FALSE,
					req.Propose.CommandId,
					state.NIL,
					req.Propose.Timestamp,
				}
				r.replica.ReplyProposeTS(propreply, req.Propose.Reply)
			}
			r.mu.Lock()
			delete(r.pendingRequests, i)
			r.mu.Unlock()
		}
	}

	if !r.replica.Dreply && config.Read_Local {
		r.proceedRead(false)
	}

	r.log.TruncateIfNeeded(to)
}

func (r *Skiff) executeCommands() {
	for !r.replica.Shutdown {
		applied := false

		for r.lastApplied < r.commitIndex {
			dlog.Print("%d applied to %d -> %d, log is %d", r.Id, r.lastApplied, r.commitIndex, r.log.Size())
			r.lastApplied++
			idx := r.lastApplied
			entry := r.log.Get(idx)
			applied = true
			// Execute command on state machine
			val := entry.Command.Execute(r.replica.State)

			r.mu.Lock()
			req, ok := r.pendingRequests[idx]
			r.mu.Unlock()
			// If this command came from a client, send the reply.
			if r.replica.Dreply && ok {
				if req.Epoch == entry.Epoch && req.Apport == entry.Apportion {
					reply := &genericsmrproto.ProposeReplyTS{
						OK:        config.TRUE,
						CommandId: req.Propose.CommandId,
						Value:     val,
						Timestamp: req.Propose.Timestamp,
					}
					r.replica.ReplyProposeTS(reply, req.Propose.Reply)
				} else {
					reply := &genericsmrproto.ProposeReplyTS{
						OK:        config.FALSE,
						CommandId: req.Propose.CommandId,
						Value:     state.NIL,
						Timestamp: req.Propose.Timestamp,
					}
					r.replica.ReplyProposeTS(reply, req.Propose.Reply)
				}

				// Safe cleanup
				r.mu.Lock()
				delete(r.pendingRequests, idx)
				r.mu.Unlock()
			}
		}

		if r.replica.Dreply && config.Read_Local {
			r.proceedRead(true)
		}

		if !applied {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (r *Skiff) handlePropose(propose *genericsmr.Propose) {
	if r.role != Leader {
		preply := &genericsmrproto.ProposeReplyTS{config.FALSE, propose.CommandId, state.NOTLeader, 0}
		r.replica.ReplyProposeTS(preply, propose.Reply)
		return
	}

	// Start batch
	batchSize := len(r.proposeChan) + 1
	if batchSize > config.MAX_BATCH {
		batchSize = config.MAX_BATCH
	}

	r.mu.Lock()

	// First proposal
	startIndex := r.log.Size()
	r.log.Set(startIndex, LogEntry{
		Command:   propose.Command,
		Epoch:     r.currentEpoch,
		Apportion: r.currentApportion,
	})
	r.pendingRequests[startIndex] = &ClientRequests{
		propose,
		startIndex,
		r.currentEpoch,
		r.currentApportion,
	}
	r.log.IncSize()

	// Batch additional proposals
	for i := 1; i < batchSize; i++ {
		select {
		case prop := <-r.proposeChan:
			index := startIndex + int32(i)
			r.log.Set(index, LogEntry{
				Command:   prop.Command,
				Epoch:     r.currentEpoch,
				Apportion: r.currentApportion,
			})
			r.pendingRequests[index] = &ClientRequests{
				prop,
				index,
				r.currentEpoch,
				r.currentApportion,
			}
			r.log.IncSize()
		default:
			break
		}
	}

	r.mu.Unlock()

	r.leaderAppendEntries()
}

func (r *Skiff) handleRWPropose(propose *genericsmr.Propose) {
	if r.role != Leader {
		preply := &genericsmrproto.ProposeReplyTS{
			config.FALSE,
			propose.CommandId,
			state.NOTLeader,
			0,
		}
		r.replica.ReplyProposeTS(preply, propose.Reply)
		return
	}

	// Determine batch size
	batchSize := len(r.proposeChan) + 1
	if batchSize > config.MAX_BATCH {
		batchSize = config.MAX_BATCH
	}

	r.mu.Lock()

	// The key invariant:
	// Reads must be ordered after the last write that precedes them
	lastWriteIndex := r.commitIndex

	// Helper to append a write
	appendWrite := func(prop *genericsmr.Propose) {
		index := r.log.Size()
		r.log.Set(index, LogEntry{
			Command:   prop.Command,
			Epoch:     r.currentEpoch,
			Apportion: r.currentApportion,
		})
		r.log.IncSize()

		r.pendingRequests[index] = &ClientRequests{
			prop,
			index,
			r.currentEpoch,
			r.currentApportion,
		}
		lastWriteIndex = index
	}

	// Helper to enqueue a read
	appendRead := func(prop *genericsmr.Propose) {
		r.pendingReads = append(r.pendingReads, &ClientRequests{
			prop,
			lastWriteIndex,
			r.currentEpoch,
			r.currentApportion,
		})
	}

	// First proposal
	if propose.Command.Op == state.GET {
		appendRead(propose)
	} else {
		appendWrite(propose)
	}

	// Batch additional proposals
	for i := 1; i < batchSize; i++ {
		select {
		case prop := <-r.proposeChan:
			if prop.Command.Op == state.GET {
				appendRead(prop)
			} else {
				appendWrite(prop)
			}
		default:
			// no more proposals
			i = batchSize
		}
	}

	needReplicate := lastWriteIndex > r.commitIndex
	needRead := lastWriteIndex == r.commitIndex
	r.mu.Unlock()

	// Only replicate if we actually appended writes
	if needReplicate {
		r.leaderAppendEntries()
	}
	if needRead {
		r.proceedRead(r.replica.Dreply)
	}
}

func (r *Skiff) startElection(apportion int32) {
	r.voteReceived = 1
	r.role = Candidate
	r.currentApportion = apportion
	r.currentEpoch += 1
	r.lastCommmitIndex = r.commitIndex
	r.replica.refreshTime(r.shard)

	dlog.Println("%v start election with term <%v,%v>", r.getShardInfo(), r.currentEpoch, r.currentApportion)
	msg := &VoteAndGatherArgs{
		Epoch:           r.currentEpoch,
		CandidateId:     r.Id,
		CandidateCommit: r.lastCommmitIndex,
		Shard:           r.shard,
	}
	for _, peer := range r.peerIds {
		r.replica.SendMsg(peer, r.replica.voteAndGatherRPC, msg)
	}
}

func (r *Skiff) handleVoteAndGather(args *VoteAndGatherArgs) {
	dlog.Print("%v handle VoteAndGather %v", r.getShardInfo(), args)
	var reply VoteAndGatherReply
	if r.commitIndex >= args.CandidateCommit {
		diff, _ := r.logFrom(args.CandidateCommit)
		reply.Diff = diff
	}
	reply.PeerCommit = r.commitIndex
	reply.PeerId = r.Id
	r.peerIndex[args.CandidateId] = max(r.peerIndex[args.CandidateId], args.CandidateCommit)
	r.nextIndex[args.CandidateId] = max(r.nextIndex[args.CandidateId], args.CandidateCommit)
	//r.nextIndex[args.CandidateId] = max(r.nextIndex[args.CandidateId], r.logSize)

	if args.Epoch > r.currentEpoch || (args.Epoch == r.currentEpoch && args.Apportion > r.currentApportion) {
		r.becomeFollower(args.Epoch, args.Apportion)
		reply.OK = true
	} else {
		reply.OK = false
	}
	reply.Epoch = r.currentEpoch
	reply.Apportion = r.currentApportion
	reply.Shard = r.shard
	dlog.Print("%v voteAndGather reply: %+v", r.getShardInfo(), reply)
	r.replica.SendMsg(args.CandidateId, r.replica.voteAndGatherReplyRPC, &reply)
}

func (r *Skiff) handleVoteAndGatherReply(reply *VoteAndGatherReply) {
	dlog.Print("%v received voteAndGatherReply: %+v", r.getShardInfo(), reply)
	// Append logs
	size := int32(len(reply.Diff))
	start := r.lastCommmitIndex + 1
	r.peerIndex[reply.PeerId] = max(r.peerIndex[reply.PeerId], reply.PeerCommit) // The latest know commit
	r.nextIndex[reply.PeerId] = max(r.nextIndex[reply.PeerId], reply.PeerCommit)

	if size > 0 {
		if reply.PeerCommit == r.lastCommmitIndex && r.log.Size() < start+size {
			r.appendFrom(start, size, reply.Diff)
		}
		if reply.PeerCommit > r.lastCommmitIndex {
			r.appendFrom(start, size, reply.Diff)
			r.commitIndex = max(r.commitIndex, reply.PeerCommit)
		}
	}

	if r.role != Candidate {
		dlog.Print("%v is not candidate, return.", r.getShardInfo())
		return
	}
	if reply.Epoch > r.currentEpoch || (reply.Epoch == r.currentEpoch && reply.Apportion > r.currentApportion) {
		dlog.Print("%v term out of date in VoteAndGatherReply %v", r.getShardInfo(), reply)
		r.becomeFollower(reply.Epoch, reply.Apportion)
		return
	}
	if reply.OK {
		r.voteReceived += 1
		if r.voteReceived*2 > len(r.peerIds)+1 {
			r.becomeLeader()
		}
	}
}

// epochAndApportion returns the last log entry's epoch and apportion
// (or -1 if there's no log) for this server.
func (r *Skiff) epochAndApportion() (int32, int32) {
	if r.log.Size() > 0 {
		lastIndex := r.log.Size() - 1
		entry := r.log.Get(lastIndex)
		return entry.Epoch, entry.Apportion
	}
	return -1, -1
}

func (r *Skiff) getShardInfo() string {
	return fmt.Sprintf("|{ rep:%v-shard:%v-term:<%v,%v> }| ", r.Id, r.shard, r.currentEpoch, decodeApportion(int(r.currentApportion)))
}

func (r *Skiff) becomeFollower(epoch int32, apportion int32) {
	dlog.Print("%v becomes Follower with term=<%v,%v>", r.getShardInfo(), r.currentEpoch, r.currentApportion)
	r.role = Follower
	r.currentEpoch = epoch
	r.currentApportion = apportion
	r.replica.refreshTime(r.shard)
	r.replica.changeRole(r.shard, r.role)
}

func (r *Skiff) becomeLeader() {
	dlog.TODO("%v becomes Leader with term=<%v,%v>", r.getShardInfo(), r.currentEpoch, r.currentApportion)
	r.role = Leader
	r.replica.changeRole(r.shard, Leader)
}

func (r *Skiff) logFrom(index int32) ([]LogEntry, int32) {
	// index greater than committed range → nothing to return
	if index > r.commitIndex {
		return nil, 0
	}
	start := index + 1
	end := r.log.Size()
	if start < 0 || start >= r.log.Size() {
		return nil, 0
	}
	if end-start > MAX_VOTE_LOG_SIZE {
		end = start + MAX_VOTE_LOG_SIZE
	}
	return r.log.Slice(start, end), end - start
}

func (r *Skiff) appendFrom(start int32, size int32, entries []LogEntry) {
	for i := int32(0); i < size; i++ {
		r.log.Set(start+i, entries[i])
	}
	r.log.SetSize(start + size)
}

func (r *Skiff) leaderAppendEntries() {
	if r.role != Leader {
		return
	}
	for _, peerId := range r.peerIds {
		ni := r.nextIndex[peerId] + 1 // Go slice start
		if ni >= r.log.Size() {
			continue
		}
		end := min(r.log.Size(), ni+int32(config.MAX_BATCH))
		entries := r.log.Slice(ni, end)

		args := &AppendEntriesArgs{
			Epoch:         r.currentEpoch,
			Apportion:     r.currentApportion,
			CurrentStatus: r.replica.getCurrentApportion(),
			LeaderId:      r.Id,
			Diff:          entries,
			StartIndex:    ni - 1, // last index peer already has
			LeaderCommit:  r.commitIndex,
			Shard:         r.shard,
		}
		r.nextIndex[peerId] = end - 1
		r.replica.SendMsg(peerId, r.replica.appendEntryRPC, args)
	}
}

func (r *Skiff) handleAppendEntries(args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	reply.OK = false
	reply.Shard = r.shard
	reply.Epoch = r.currentEpoch
	reply.Apportion = r.currentApportion
	reply.FollowerId = r.Id
	r.peerIndex[args.LeaderId] = args.LeaderCommit

	if args.LeaderCommit < r.commitIndex {
		reply.OK = false
		reply.CommitIndex = r.commitIndex
		r.replica.SendMsg(args.LeaderId, r.replica.appendEntryReplyRPC, &reply)
		return
	}

	if args.Epoch > r.currentEpoch || (args.Epoch == r.currentEpoch && args.Apportion >= r.currentApportion) {
		r.becomeFollower(args.Epoch, args.Apportion)
		if args.StartIndex >= r.log.Size() {
			reply.OK = false
			reply.CommitIndex = r.commitIndex
		} else {
			size := int32(len(args.Diff))
			r.appendFrom(args.StartIndex+1, size, args.Diff)
			reply.CommitIndex = args.StartIndex + size
			reply.OK = true

			commit := min(r.log.Size(), args.LeaderCommit)
			r.commitLog(r.commitIndex, commit)
			r.commitIndex = commit
		}

		r.replica.runBalance(r.shard, args.LeaderId, args.CurrentStatus, r)
	}

	r.replica.SendMsg(args.LeaderId, r.replica.appendEntryReplyRPC, &reply)
}

func (r *Skiff) handleAppendEntriesReply(reply *AppendEntriesReply) {
	r.peerIndex[reply.FollowerId] = reply.CommitIndex
	if reply.Epoch > r.currentEpoch || (reply.Epoch == r.currentEpoch && reply.Apportion > r.currentApportion) {
		r.becomeFollower(reply.Epoch, reply.Apportion)
		return
	}
	if !reply.OK {
		r.nextIndex[reply.FollowerId] = reply.CommitIndex // Resend from its commit index
	}
	r.updateCommitIndex()
}

func (r *Skiff) startBalance(leaderId int32) {
	r.tokenAcquired = false
	r.balanceReceived = 1

	dlog.Println("%v start balance with term <%v,%v>", r.getShardInfo(), r.currentEpoch, r.currentApportion)
	msg := &BalanceArgs{
		Shard:       r.shard,
		LeaderId:    leaderId,
		Sender:      r.Id,
		ProposalNum: r.replica.getCurrentApportion(),
	}
	for _, peer := range r.peerIds {
		r.replica.SendMsg(peer, r.replica.balanceRPC, msg)
	}
}

func (r *Skiff) handleBalance(args *BalanceArgs) {
	var reply BalanceReply
	reply.Shard = r.shard
	if r.role == Leader && r.Id == args.LeaderId && r.grantedApportion < args.ProposalNum {
		reply.Token = true
		r.grantedApportion = args.ProposalNum
		time.AfterFunc(time.Duration(*config.TokenRegenerate)*time.Millisecond, func() {
			r.grantedApportion = 0
		})
	}
	r.replica.SendMsg(args.Sender, r.replica.balanceReplyRPC, &reply)
}

func (r *Skiff) handleBalanceReply(reply *BalanceReply) {
	r.balanceReceived += 1
	if reply.Token {
		r.tokenAcquired = true
	}
	if r.balanceReceived*2 > len(r.peerIds)+1 && r.tokenAcquired {
		r.tokenAcquired = false //Reset it false to avoid duplicate balancing
		if r.replica.allowBalance(r.shard) {
			r.startElection(r.replica.getCurrentApportion())
		}
	}
}

func (r *Skiff) updateCommitIndex() {
	peers := len(r.peerIds)
	values := make([]int, peers+1)
	i := 0
	for _, value := range r.peerIndex {
		values[i] = int(value)
		i++
	}
	values[peers] = int(r.log.Size()) // this replica's log size

	// find median from the values to represent majority commits
	sort.Ints(values)
	n := peers + 1
	majorityIndex := 0
	if n%2 == 1 {
		majorityIndex = values[n/2]
	} else {
		majorityIndex = values[n/2-1]
	}
	from := r.commitIndex
	r.commitIndex = max(r.commitIndex, int32(majorityIndex))
	r.commitLog(from, r.commitIndex)
}

func (r *Skiff) proceedRead(execute bool) {
	r.mu.Lock()
	reads := r.pendingReads      // copy the slice reference
	commitIndex := r.commitIndex // snapshot commit index
	r.mu.Unlock()

	count := 0
	for _, req := range reads {
		if req.Index <= commitIndex {
			count++
			var val state.Value
			if execute {
				val = req.Propose.Command.Execute(r.replica.State)
			} else {
				val = state.NIL
			}
			preply := &genericsmrproto.ProposeReplyTS{
				config.TRUE,
				req.Propose.CommandId,
				val,
				0,
			}
			r.replica.ReplyProposeTS(preply, req.Propose.Reply)
		} else {
			break // since reads are ordered by index, we can stop
		}
	}

	if count > 0 {
		r.mu.Lock()
		r.pendingReads = r.pendingReads[count:]
		r.mu.Unlock()
	}
}
