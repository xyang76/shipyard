package multiraft

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/raft"
	"Mix/state"
	"fmt"
	"sort"
	"sync"
	"time"
)

type RaftState int

const (
	Null RaftState = iota
	Follower
	Candidate
	Leader
	Dead
)

type ShardedRaft struct {
	replica *Replica
	role    RaftState
	Id      int32
	peerIds []int32
	shard   int32
	mu      sync.Mutex
	replymu sync.Mutex

	// pre-allocated log
	//log             []raft.LogEntry
	//logSize         int32 // tracks the number of valid entries
	log             *raft.TruncatedLog
	proposeChan     chan *genericsmr.Propose
	pendingRequests map[int32]*raft.ClientRequests
	pendingReads    []*raft.ClientRequests

	commitIndex  int32
	lastApplied  int32
	votedFor     int32
	voteReceived int
	currentTerm  int32

	nextIndex     map[int32]int32
	matchIndex    map[int32]int32
	logAppendChan chan struct{}

	electionResetEvent time.Time
	electionLastTerm   int32
}

func NewShardedRaft(repl *Replica, id int32, peerIds []int32, shard int32) *ShardedRaft {
	r := &ShardedRaft{
		replica: repl,
		Id:      id,
		peerIds: peerIds,
		shard:   shard,
		//log:             make([]raft.LogEntry, 15*1024*1024), // preallocate 15M entries
		//logSize:         0,                                   // initially empty
		log:             raft.NewTruncatedLog(id, config.LOG_SIZE),
		logAppendChan:   make(chan struct{}, config.CHAN_BUFFER_SIZE),
		proposeChan:     make(chan *genericsmr.Propose, config.CHAN_BUFFER_SIZE),
		pendingRequests: make(map[int32]*raft.ClientRequests),
		commitIndex:     -1,
		nextIndex:       make(map[int32]int32),
		matchIndex:      make(map[int32]int32),
		role:            Candidate,
		votedFor:        -1,
		lastApplied:     -1,
	}

	if config.Read_Local {
		r.pendingReads = make([]*raft.ClientRequests, 0, config.LOG_SIZE)
	}

	go r.run()
	return r
}

func (r *ShardedRaft) run() {
	if r.replica.Exec {
		go r.executeCommands()
	}

	go r.runElectionTimer()

	onOffProposeChan := r.proposeChan

	//tick := time.NewTicker(time.Duration(*config.TickTime) * time.Millisecond)
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

func (r *ShardedRaft) startElection() {
	r.role = Candidate
	r.currentTerm += 1
	r.electionResetEvent = time.Now()
	r.votedFor = r.Id
	r.voteReceived = 1
	lastIndex, lastTerm := r.lastLogIndexAndTerm()
	dlog.Print("%v start election with term %v", r.getShardInfo(), r.currentTerm)
	msg := &raft.RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.Id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
		Shard:        r.shard,
	}
	for _, peer := range r.peerIds {
		r.replica.SendMsg(peer, r.replica.requestVoteRPC, msg)
	}
}

func (r *ShardedRaft) runElectionTimer() {
	go func() {
		for {
			timeoutDuration := config.RandomElectionTimeout()
			timer := time.NewTimer(timeoutDuration)

			select {
			case <-timer.C:
				//r.mu.Lock()
				state := r.role
				elapsed := time.Since(r.electionResetEvent)
				//lastTerm := r.electionLastTerm
				r.electionLastTerm = r.currentTerm
				//r.mu.Unlock()

				timeoutDuration := config.RandomElectionTimeout() // randomized

				if state == Candidate || state == Follower {
					if elapsed >= timeoutDuration {
						dlog.Print("%v restart election due to timeout %v:%v", r.getShardInfo(), elapsed, timeoutDuration)
						r.startElection()
					}
				}
				timer.Reset(time.Duration(*config.RaftElectionInterval)) // or some random interval
			}
		}
	}()
}

func (r *ShardedRaft) handleRequestVote(args *raft.RequestVoteArgs) {
	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()
	dlog.Print("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, r.currentTerm, r.votedFor, lastLogIndex, lastLogTerm)
	if args.Term > r.currentTerm {
		dlog.Print("%v ... term out of date in RequestVote", r.getShardInfo())
		r.becomeFollower(args.Term)
	}
	var reply raft.RequestVoteReply
	if r.currentTerm == args.Term &&
		(r.votedFor == -1 || r.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		r.votedFor = args.CandidateId
		r.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = r.currentTerm
	reply.Shard = r.shard
	dlog.Print("%v ... RequestVote reply: %+v", r.getShardInfo(), reply)
	r.replica.SendMsg(args.CandidateId, r.replica.requestVoteReplyRPC, &reply)
}

func (r *ShardedRaft) handleRequestVoteReply(reply *raft.RequestVoteReply) {
	term := r.currentTerm
	dlog.Print("%v received requestVoteReply: %+v", r.getShardInfo(), reply)
	if r.role != Candidate {
		dlog.Print("%v while waiting for reply, role = %v", r.getShardInfo(), r.role)
		return
	}
	if reply.Term > term {
		dlog.Print("%v term out of date in RequestVoteReply", r.getShardInfo())
		r.becomeFollower(reply.Term)
		return
	} else if reply.Term == term {
		if reply.VoteGranted {
			r.voteReceived += 1
			if r.voteReceived*2 > len(r.peerIds)+1 {
				// Won the election!
				dlog.Print("%v wins election with %d votes", r.getShardInfo(), r.voteReceived)
				r.becomeLeader()
				return
			}
		}
	}
}

func (r *ShardedRaft) handleAppendEntries(args *raft.AppendEntriesArgs) {
	dlog.Print("%v appendEntries: %+v", r.getShardInfo(), args)

	var reply raft.AppendEntriesReply
	reply.Success = false
	reply.LastLogIndex = r.log.Size() - 1

	if args.Term > r.currentTerm {
		dlog.Print("... term out of date in AppendEntries")
		r.becomeFollower(args.Term)
	}

	if args.Term == r.currentTerm {
		if r.role != Follower {
			r.becomeFollower(args.Term)
		}
		r.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < r.log.Size() && args.PrevLogTerm == r.log.Get(args.PrevLogIndex).Term) {

			reply.Success = true
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			// skip already matching entries
			for logInsertIndex < r.log.Size() && newEntriesIndex < len(args.Entries) {
				if r.log.Get(logInsertIndex).Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// calculate gap
			latestIndex := args.PrevLogIndex + int32(len(args.Entries))
			gap := latestIndex - r.log.Size()

			if config.Fake_recovery && gap > 1000 {
				// --- fake catch-up for performance testing ---
				if latestIndex > r.log.Size() {
					r.log.SetSize(latestIndex) // jump logSize to leader's latest index
				}
				if args.LeaderCommit > r.commitIndex {
					r.commitIndex = min(args.LeaderCommit, r.log.Size()-1)
					// skip commitLog() since we don't execute commands
				}
			} else {
				// append remaining entries normally
				for i := newEntriesIndex; i < len(args.Entries); i++ {
					r.log.Set(logInsertIndex, args.Entries[i])
					logInsertIndex++
				}

				if logInsertIndex > r.log.Size() {
					r.log.SetSize(logInsertIndex)
				}
				dlog.Print("... log is now: %v", r.log)

				// update commit index and apply entries
				if args.LeaderCommit > r.commitIndex {
					from := r.commitIndex
					r.commitIndex = min(args.LeaderCommit, r.log.Size()-1)
					dlog.Print("... setting commitIndex=%d", r.commitIndex)
					r.commitLog(from, r.commitIndex)
				}
			}
		}
	}

	// send reply once at the end
	reply.FollowerId = r.Id
	reply.Term = r.currentTerm
	reply.Shard = r.shard
	reply.LastLogIndex = r.log.Size() - 1
	dlog.Print("AppendEntries reply: %+v", reply)
	r.replica.SendMsg(args.LeaderId, r.replica.appendEntryReplyRPC, &reply)
}

func (r *ShardedRaft) appendFrom(start int32, size int32, entries []raft.LogEntry) {
	for i := int32(0); i < size; i++ {
		r.log.Set(start+i, entries[i])
	}
	r.log.SetSize(start + size)
}

func (r *ShardedRaft) handleAppendEntriesReply(reply *raft.AppendEntriesReply) {
	if reply.Term > r.currentTerm {
		dlog.Print("term out of date in heartbeat reply")
		r.becomeFollower(reply.Term)
		return
	}
	peerId := reply.FollowerId
	if r.role == Leader && r.currentTerm == reply.Term {
		if reply.Success {
			// follower accepted entries up to LastLogIndex
			r.mu.Lock()
			r.matchIndex[peerId] = reply.LastLogIndex
			r.nextIndex[peerId] = reply.LastLogIndex + 1
			r.mu.Unlock()

			dlog.Print("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v",
				peerId, r.nextIndex, r.matchIndex)

			// Leader commits
			if *config.FastRaft == 1 {
				r.updateCommitIndexRaft()
			} else {
				savedCommitIndex := r.commitIndex
				for i := r.commitIndex + 1; i < r.log.Size(); i++ {
					if r.log.Get(i).Term == r.currentTerm {
						matchCount := 1
						for _, pid := range r.peerIds {
							if r.matchIndex[pid] >= i {
								matchCount++
							}
						}
						if matchCount*2 > len(r.peerIds)+1 {
							r.commitIndex = i
						}
					}
				}

				if r.commitIndex != savedCommitIndex {
					dlog.Print("leader sets commitIndex := %d", r.commitIndex)
					// r.newCommitReadyChan <- struct{}{}
					r.commitLog(savedCommitIndex, r.commitIndex)
				}
			}
		} else {

			// === NEW: use reply.LastLogIndex for fast rollback ===
			// reply.LastLogIndex is the last index that matches the follower’s log.
			newNextIndex := reply.LastLogIndex + 1

			if newNextIndex < 1 {
				newNextIndex = 1
			}
			if newNextIndex > r.log.Size() {
				newNextIndex = r.log.Size()
			}

			r.mu.Lock()
			r.nextIndex[peerId] = newNextIndex
			r.mu.Unlock()

			dlog.Print("AppendEntries reply from %d !success: follower last log = %d, nextIndex := %d",
				peerId, reply.LastLogIndex, newNextIndex)
		}
	}
}

func (r *ShardedRaft) updateCommitIndexRaft() {
	peers := len(r.peerIds)

	// Collect matchIndex from followers + leader
	values := make([]int, 0, peers+1)
	for _, pid := range r.peerIds {
		values = append(values, int(r.matchIndex[pid]))
	}
	values = append(values, int(r.log.Size()-1)) // leader itself

	// Sort to find majority index
	sort.Ints(values)

	n := peers + 1
	majorityIndex := values[n/2] // works for both even and odd

	// Raft rule: only commit entries from current term
	// Walk backward until term matches or commitIndex reached
	ci := r.commitIndex

	if r.log.Get(int32(majorityIndex)).Term == r.currentTerm {
		r.commitIndex = max(int32(majorityIndex), r.commitIndex)
	} else {
		for i := int32(majorityIndex); i > ci; i-- {
			if r.log.Get(i).Term == r.currentTerm {
				r.commitIndex = i
				break
			}
		}
	}

	//for i := int32(majorityIndex); i > ci; i-- {
	//	if r.log.Get(i).Term == r.currentTerm {
	//		r.commitIndex = i
	//		break
	//	}
	//}

	if r.commitIndex > ci {
		r.commitLog(ci, r.commitIndex)
	}
}

func (r *ShardedRaft) becomeLeader() {
	dlog.TODO("%v becomes Leader with term=%d", r.getShardInfo(), r.currentTerm)
	r.role = Leader
	r.mu.Lock()
	for _, peerId := range r.peerIds {
		r.nextIndex[peerId] = r.log.Size()
		r.matchIndex[peerId] = -1
	}
	r.mu.Unlock()

	dlog.Print("becomes Leader; info=%v, term=%d, nextIndex=%v, matchIndex=%v", r.getShardInfo(), r.currentTerm, r.nextIndex, r.matchIndex)

	go func() {
		heartbeat := time.Duration(*config.HeartBeatInterval) * time.Millisecond
		r.leaderAppendEntries()
		t := time.NewTimer(heartbeat)
		defer t.Stop()

		for {
			doSend := false

			select {
			case <-t.C:
				// periodic heartbeat
				doSend = true

				// reset timer
				t.Stop()
				t.Reset(heartbeat)

			case _, ok := <-r.logAppendChan:
				// triggered because new log entries were appended
				if ok {
					doSend = true
				} else {
					// channel closed means shutdown
					return
				}
				// reset timer (Raft: leader should wait full heartbeat after sending)
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeat)
			}
			if doSend {
				//r.mu.Lock()
				if r.role != Leader {
					//r.mu.Unlock()
					return
				}
				//r.mu.Unlock()
				r.leaderAppendEntries()
			}
		}
	}()
}

func (r *ShardedRaft) becomeFollower(term int32) {
	dlog.Print("%v becomes Follower with term=%d", r.getShardInfo(), term)
	r.role = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.electionResetEvent = time.Now()
}

func (r *ShardedRaft) leaderAppendEntries() {
	if r.role != Leader {
		return
	}
	for _, peerId := range r.peerIds {
		r.mu.Lock()
		ni := r.nextIndex[peerId]
		r.mu.Unlock()
		prevLogIndex := ni - 1
		prevLogTerm := int32(-1)
		if prevLogIndex >= 0 {
			prevLogTerm = r.log.Get(prevLogIndex).Term
		}
		// Limit entries: r.log[ni : ni+MAX_BATCH]
		end := min(r.log.Size(), ni+int32(config.MAX_BATCH))
		entries := r.log.Slice(ni, end)

		args := &raft.AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderId:     r.Id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
			Shard:        r.shard,
		}
		//r.mu.Unlock()
		//if len(entries) > 0 {
		//	dlog.EC.Count("AppendEntries", "%v leader appends", r.getShardInfo())
		//}
		r.mu.Lock()
		r.nextIndex[peerId] = end - 1
		r.mu.Unlock()
		r.replica.SendMsg(peerId, r.replica.appendEntryRPC, args)
	}
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked.
func (r *ShardedRaft) lastLogIndexAndTerm() (int32, int32) {
	if r.log.Size() > 0 {
		lastIndex := r.log.Size() - 1
		return lastIndex, int32(r.log.Get(lastIndex).Term)
	} else {
		return -1, -1
	}
}

// If not execute commands, server will directly reply based on commitLog function
func (r *ShardedRaft) commitLog(from int32, to int32) {
	dlog.Print("%v commiting log from %d to %d, with log size %d", r.getShardInfo(), from, to, r.log.Size())
	if r.replica.Dreply {
		return
	}
	for i := from + 1; i <= to; i++ {
		r.mu.Lock()
		req := r.pendingRequests[i]
		r.mu.Unlock()
		if req != nil {
			if req.Term == r.log.Get(i).Term && req.Index == i {
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
	//r.proceedRead(false)
	r.log.TruncateIfNeeded(to)
}

func (r *ShardedRaft) executeCommands() {
	for !r.replica.Shutdown {
		applied := false

		for r.lastApplied < r.commitIndex && r.lastApplied+1 < r.log.Size() {
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
				if req.Term == entry.Term {
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

func (r *ShardedRaft) getShardInfo() string {
	return fmt.Sprintf("|{ rep:%v-shard:%v-term:%v }| ", r.Id, r.shard, r.currentTerm)
}

func (r *ShardedRaft) handlePropose(propose *genericsmr.Propose) {
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
	r.log.Set(startIndex, raft.LogEntry{
		Command: propose.Command,
		Term:    r.currentTerm,
	})
	r.log.IncSize()
	r.pendingRequests[startIndex] = &raft.ClientRequests{
		propose,
		startIndex,
		r.currentTerm,
	}

	// Batch additional proposals
	for i := 1; i < batchSize; i++ {
		select {
		case prop := <-r.proposeChan:
			index := startIndex + int32(i)
			r.log.Set(index, raft.LogEntry{
				Command: prop.Command,
				Term:    r.currentTerm,
			})
			r.pendingRequests[index] = &raft.ClientRequests{
				prop,
				index,
				r.currentTerm,
			}
			r.log.IncSize()
		default:
			break
		}
	}

	r.mu.Unlock()

	r.leaderAppendEntries()
}

func (r *ShardedRaft) handleRWPropose(propose *genericsmr.Propose) {
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
		r.log.Set(index, raft.LogEntry{
			Command: prop.Command,
			Term:    r.currentTerm,
		})
		r.log.IncSize()

		r.pendingRequests[index] = &raft.ClientRequests{
			prop,
			index,
			r.currentTerm,
		}
		lastWriteIndex = index
	}

	// Helper to enqueue a read
	appendRead := func(prop *genericsmr.Propose) {
		r.pendingReads = append(r.pendingReads, &raft.ClientRequests{
			prop,
			lastWriteIndex,
			r.currentTerm,
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

func (r *ShardedRaft) proceedRead(execute bool) {
	r.replymu.Lock()
	defer r.replymu.Unlock()

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
