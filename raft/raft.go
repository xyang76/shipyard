package raft

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/fastrpc"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/state"
	"sort"
	"sync"
	"time"
)

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
	Dead
)

// Raft replica
type Replica struct {
	*genericsmr.Replica
	role    RaftState
	peerIds []int32
	mu      sync.Mutex
	replymu sync.Mutex

	requestVoteChan        chan fastrpc.Serializable
	appendEntriesChan      chan fastrpc.Serializable
	requestVoteReplyChan   chan fastrpc.Serializable
	appendEntriesReplyChan chan fastrpc.Serializable

	// pre-allocated log
	//log             []LogEntry
	//logSize         int32 // tracks the number of valid entries
	log             *TruncatedLog
	pendingRequests map[int32]*ClientRequests
	pendingReads    []*ClientRequests

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

	requestVoteRPC      uint8
	requestVoteReplyRPC uint8
	appendEntryRPC      uint8
	appendEntryReplyRPC uint8
	Shutdown            bool
}

type ClientRequests struct {
	Propose *genericsmr.Propose
	Index   int32
	Term    int32
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
	r := &Replica{
		Replica: genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		//log:                    make([]LogEntry, 15*1024*1024), // preallocate 15M entries
		//logSize:                0,                              // initially empty
		log:                    NewTruncatedLog(int32(id), 0, config.LOG_SIZE),
		logAppendChan:          make(chan struct{}, config.CHAN_BUFFER_SIZE),
		requestVoteChan:        make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		requestVoteReplyChan:   make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		appendEntriesChan:      make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		appendEntriesReplyChan: make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
		pendingRequests:        make(map[int32]*ClientRequests),
		commitIndex:            -1,
		nextIndex:              make(map[int32]int32),
		matchIndex:             make(map[int32]int32),
		role:                   Candidate,
		votedFor:               -1,
		lastApplied:            -1,
		Shutdown:               false,
	}

	if config.Read_Local {
		r.pendingReads = make([]*ClientRequests, 0, config.LOG_SIZE)
	}

	r.requestVoteRPC = r.RegisterRPC(new(RequestVoteArgs), r.requestVoteChan)
	r.requestVoteReplyRPC = r.RegisterRPC(new(RequestVoteReply), r.requestVoteReplyChan)
	r.appendEntryRPC = r.RegisterRPC(new(AppendEntriesArgs), r.appendEntriesChan)
	r.appendEntryReplyRPC = r.RegisterRPC(new(AppendEntriesReply), r.appendEntriesReplyChan)

	go r.run()
	return r
}

// RPC: external controller can call to mark this node as leader
func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
	r.role = Leader
	return nil
}

// main loop: handle client proposals and other future RPCs
func (r *Replica) initPeers() {
	r.peerIds = make([]int32, 0, r.N-1)
	for i := int32(0); i < int32(r.N); i++ {
		if i == r.Id {
			continue
		}
		r.peerIds = append(r.peerIds, i)
	}
}

/* ============= */

var clockChan chan bool

func (r *Replica) clock() {
	for !r.Shutdown {
		time.Sleep(1000 * 1000 * 5)
		clockChan <- true
	}
}

// main loop: handle client proposals and other future RPCs
func (r *Replica) run() {
	// wait until all RPC IDs are nonzero
	for r.requestVoteRPC == 0 || r.requestVoteReplyRPC == 0 ||
		r.appendEntryRPC == 0 || r.appendEntryReplyRPC == 0 {
		time.Sleep(time.Microsecond)
	}

	r.ConnectToPeers()
	r.initPeers()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	if r.Exec {
		go r.executeCommands()
	}

	dlog.Println("RaftReplica running, waiting for client proposes")
	onOffProposeChan := r.ProposeChan

	tick := time.NewTicker(time.Duration(*config.TickTime) * time.Millisecond)
	defer tick.Stop()

	go r.runElectionTimer()

	for !r.Shutdown {
		select {
		case <-tick.C:
			onOffProposeChan = r.ProposeChan
			break

		case prop := <-onOffProposeChan:
			// got a client propose
			dlog.Print("RaftReplica got proposal Op=%d Id=%d\n", prop.Command.Op, prop.CommandId)
			if config.Read_Local {
				r.handleRWPropose(prop)
			} else {
				r.handlePropose(prop)
			}
			// after receiving a proposal, optionally pause accepting more to prioritize internal messages
			onOffProposeChan = nil
			break

		case reqVoteS := <-r.requestVoteChan:
			reqVote := reqVoteS.(*RequestVoteArgs)
			//got an Accept message
			dlog.Printf("Received RequestVote from replica %d, with term %d\n", reqVote.CandidateId, reqVote.Term)
			r.handleRequestVote(reqVote)
			break

		case reqVoteReplyS := <-r.requestVoteReplyChan:
			reqVoteReply := reqVoteReplyS.(*RequestVoteReply)
			dlog.Printf("Received RequestVoteReply term %d, voteGranted=%v\n",
				reqVoteReply.Term, reqVoteReply.VoteGranted)
			r.handleRequestVoteReply(reqVoteReply)
			break

		case appendEntriesS := <-r.appendEntriesChan:
			appendEntries := appendEntriesS.(*AppendEntriesArgs)
			dlog.Printf("Received AppendEntries from replica %d, term %d, leaderCommit=%d\n",
				appendEntries.LeaderId, appendEntries.Term, appendEntries.LeaderCommit)
			r.handleAppendEntries(appendEntries)
			break

		case appendEntriesReplyS := <-r.appendEntriesReplyChan:
			appendEntriesReply := appendEntriesReplyS.(*AppendEntriesReply)
			dlog.Printf("Received AppendEntriesReply from replica %d, term %d, success=%v, LastLogIndex=%d\n",
				appendEntriesReply.Term,
				appendEntriesReply.Success, appendEntriesReply.LastLogIndex)
			r.handleAppendEntriesReply(appendEntriesReply)
			break
		}
	}

}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked.
func (r *Replica) lastLogIndexAndTerm() (int32, int32) {
	if r.log.Size() > 0 {
		lastIndex := r.log.Size() - 1
		return lastIndex, int32(r.log.Get(lastIndex).Term)
	} else {
		return -1, -1
	}
}

func (r *Replica) startElection() {
	r.role = Candidate
	r.currentTerm += 1
	r.electionResetEvent = time.Now()
	r.votedFor = r.Id
	r.voteReceived = 1
	lastIndex, lastTerm := r.lastLogIndexAndTerm()
	msg := &RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.Id,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	dlog.Info("%d starting leader election ", r.Id)
	for _, peer := range r.peerIds {
		r.SendMsg(peer, r.requestVoteRPC, msg)
	}
}

func (r *Replica) runElectionTimer() {

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
						dlog.Info("%d restart election due to timeout %v:%v", r.Id, elapsed, timeoutDuration)
						r.startElection()
					}
				}
				timer.Reset(time.Duration(*config.RaftElectionInterval)) // or some random interval
			}
		}
	}()
}

func (r *Replica) handleRequestVote(args *RequestVoteArgs) {
	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()
	dlog.Print("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, r.currentTerm, r.votedFor, lastLogIndex, lastLogTerm)
	if args.Term > r.currentTerm {
		dlog.Print("... term out of date in RequestVote")
		r.becomeFollower(args.Term)
	}
	var reply RequestVoteReply
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
	dlog.Print("... RequestVote reply: %+v", reply)
	r.SendMsg(args.CandidateId, r.requestVoteReplyRPC, &reply)
}

func (r *Replica) handleRequestVoteReply(reply *RequestVoteReply) {
	//r.mu.Lock()
	term := r.currentTerm
	//r.mu.Unlock()
	dlog.Print("%d received requestVoteReply: %+v", r.Id, reply)
	if r.role != Candidate {
		dlog.Println("while waiting for reply, role = %v", r.role)
		return
	}
	if reply.Term > term {
		dlog.Println("term out of date in RequestVoteReply")
		r.becomeFollower(reply.Term)
		return
	} else if reply.Term == term {
		if reply.VoteGranted {
			r.voteReceived += 1
			if r.voteReceived*2 > len(r.peerIds)+1 {
				// Won the election!
				dlog.Println("wins election with %d votes", r.voteReceived)
				r.becomeLeader()
				return
			}
		}
	}
}

func (r *Replica) handleAppendEntries(args *AppendEntriesArgs) {
	dlog.Print("%d appendEntries: %+v", r.Id, args)
	var reply AppendEntriesReply
	reply.Success = false
	reply.LastLogIndex = r.log.Size() - 1

	if args.Term > r.currentTerm {
		r.becomeFollower(args.Term)
	}

	if args.Term == r.currentTerm {
		if r.role != Follower {
			r.becomeFollower(args.Term)
		}
		r.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 || (args.PrevLogIndex < r.log.Size() && args.PrevLogTerm == r.log.Get(args.PrevLogIndex).Term) {
			reply.Success = true
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for logInsertIndex < r.log.Size() && newEntriesIndex < len(args.Entries) {
				if r.log.Get(logInsertIndex).Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			// fake recovery for large gap
			latestIndex := args.PrevLogIndex + int32(len(args.Entries))
			gap := latestIndex - r.log.Size()
			if config.Fake_recovery && gap > 1000 {
				r.log.SetSize(latestIndex)
				if args.LeaderCommit > r.commitIndex {
					r.commitIndex = min(args.LeaderCommit, r.log.Size()-1)
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

				// update commit index
				if args.LeaderCommit > r.commitIndex {
					from := r.commitIndex
					r.commitIndex = min(args.LeaderCommit, r.log.Size()-1)
					r.commitLog(from, r.commitIndex)
				}
			}
		}
	}

	reply.FollowerId = r.Id
	reply.Term = r.currentTerm
	reply.LastLogIndex = r.log.Size() - 1
	dlog.Print("AppendEntries reply: %+v", reply)
	r.SendMsg(args.LeaderId, r.appendEntryReplyRPC, &reply)
}

func (r *Replica) appendFrom(start int32, size int32, entries []LogEntry) {
	for i := int32(0); i < size; i++ {
		r.log.Set(start+i, entries[i])
	}
	r.log.SetSize(start + size)
}

func (r *Replica) handleAppendEntriesReply(reply *AppendEntriesReply) {
	if reply.Term > r.currentTerm {
		dlog.Print("term out of date in heartbeat reply")
		r.becomeFollower(reply.Term)
		return
	}
	peerId := reply.FollowerId
	if r.role == Leader && r.currentTerm == reply.Term {
		if reply.Success {
			// follower accepted entries up to LastLogIndex
			r.matchIndex[peerId] = reply.LastLogIndex
			r.nextIndex[peerId] = reply.LastLogIndex + 1

			dlog.Print("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v",
				peerId, r.nextIndex, r.matchIndex)

			// Leader commits
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

			r.nextIndex[peerId] = newNextIndex

			dlog.Print("AppendEntries reply from %d !success: follower last log = %d, nextIndex := %d",
				peerId, reply.LastLogIndex, newNextIndex)
		}
	}
}

func (r *Replica) becomeLeader() {
	dlog.TODO("%d becomes Leader with term=%d", r.Id, r.currentTerm)
	r.role = Leader
	for _, peerId := range r.peerIds {
		r.nextIndex[peerId] = r.log.Size()
		r.matchIndex[peerId] = -1
	}
	dlog.Print("becomes Leader; id=%d, term=%d, nextIndex=%v, matchIndex=%v", r.Id, r.currentTerm, r.nextIndex, r.matchIndex)

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

func (r *Replica) becomeFollower(term int32) {
	dlog.Print("%d becomes Follower with term=%d", r.Id, term)
	r.role = Follower
	r.currentTerm = term
	r.votedFor = -1
	r.electionResetEvent = time.Now()
}

func (r *Replica) leaderAppendEntries() {
	if r.role != Leader {
		return
	}
	for _, peerId := range r.peerIds {
		//r.mu.Lock()
		r.mu.Lock()
		ni := r.nextIndex[peerId]
		r.mu.Unlock()
		prevLogIndex := ni - 1
		prevLogTerm := int32(-1)
		if prevLogIndex >= 0 {
			prevLogTerm = r.log.Get(prevLogIndex).Term
		}
		// Limit entries: r.log[ni : ni+MAX_BATCH]
		//entries := r.log.Slice(ni, r.log.Size())
		//if len(entries) > config.MAX_BATCH {
		//	entries = entries[:config.MAX_BATCH]
		//}
		end := min(r.log.Size(), ni+int32(config.MAX_BATCH))
		entries := r.log.Slice(ni, end)

		args := &AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderId:     r.Id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: r.commitIndex,
		}
		r.mu.Lock()
		r.nextIndex[peerId] = end - 1
		r.mu.Unlock()
		r.SendMsg(peerId, r.appendEntryRPC, args)
	}
}

// Minimal client-proposal handler: if not leader, reply FALSE; if leader append to local log and reply TRUE
func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	dlog.Print("%d :%v received propose %v\n", r.Id, r.role, propose.Command)

	if r.role != Leader {
		preply := &genericsmrproto.ProposeReplyTS{config.FALSE, propose.CommandId, state.NOTLeader, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}
	// Special command for clients identify the leader, not involving replication
	if propose.CommandId == config.IdentifyLeader && propose.Command.Op == state.GET {
		preply := &genericsmrproto.ProposeReplyTS{config.TRUE, config.IdentifyLeader, state.ISLeader, 0}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	batchSize := len(r.ProposeChan) + 1
	if batchSize > config.MAX_BATCH {
		batchSize = config.MAX_BATCH
	}

	startIndex := r.log.Size()
	entries := make([]LogEntry, batchSize)
	entries[0] = LogEntry{Command: propose.Command, Term: r.currentTerm}
	r.pendingRequests[startIndex] = &ClientRequests{propose, startIndex, r.currentTerm}

	for i := 1; i < batchSize; i++ {
		prop := <-r.ProposeChan
		idx := startIndex + int32(i)
		entries[i] = LogEntry{Command: prop.Command, Term: r.currentTerm}
		r.pendingRequests[idx] = &ClientRequests{prop, idx, r.currentTerm}
	}

	// append to preallocated log
	for i := 0; i < batchSize; i++ {
		r.log.Set(r.log.Size(), entries[i])
		r.log.IncSize()
	}

	//r.leaderAppendEntries()
	r.leaderAppendEntries()

}

func (r *Replica) handleRWPropose(propose *genericsmr.Propose) {
	// Not leader → reject
	if r.role != Leader {
		preply := &genericsmrproto.ProposeReplyTS{
			config.FALSE,
			propose.CommandId,
			state.NOTLeader,
			0,
		}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	// Special command: identify leader (no replication)
	if propose.CommandId == config.IdentifyLeader {
		preply := &genericsmrproto.ProposeReplyTS{
			config.TRUE,
			config.IdentifyLeader,
			state.ISLeader,
			0,
		}
		r.ReplyProposeTS(preply, propose.Reply)
		return
	}

	// Batch size
	batchSize := len(r.ProposeChan) + 1
	if batchSize > config.MAX_BATCH {
		batchSize = config.MAX_BATCH
	}

	r.mu.Lock()

	// Reads must follow the last preceding write
	lastWriteIndex := r.commitIndex

	// Append write helper
	appendWrite := func(prop *genericsmr.Propose) {
		index := r.log.Size()
		r.log.Set(index, LogEntry{
			Command: prop.Command,
			Term:    r.currentTerm,
		})
		r.log.IncSize()

		r.pendingRequests[index] = &ClientRequests{
			prop,
			index,
			r.currentTerm,
		}
		lastWriteIndex = index
	}

	// Append read helper
	appendRead := func(prop *genericsmr.Propose) {
		r.pendingReads = append(r.pendingReads, &ClientRequests{
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

	// Batch remaining proposals
	for i := 1; i < batchSize; i++ {
		select {
		case prop := <-r.ProposeChan:
			if prop.Command.Op == state.GET {
				appendRead(prop)
			} else {
				appendWrite(prop)
			}
		default:
			i = batchSize // exit loop
		}
	}

	needReplicate := lastWriteIndex > r.commitIndex
	needRead := lastWriteIndex == r.commitIndex
	r.mu.Unlock()

	// Trigger replication only if writes were appended
	if needReplicate {
		//r.logAppendChan <- struct{}{}
		r.leaderAppendEntries()
	}
	if needRead {
		r.proceedRead(r.Dreply)
	}
}

func (r *Replica) updateCommitIndexRaft() {
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
	//for i := int32(majorityIndex); i > ci; i-- {
	//	if r.log.Get(i).Term == r.currentTerm {
	//		r.commitIndex = i
	//		break
	//	}
	//}

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

	if r.commitIndex > ci {
		r.commitLog(ci, r.commitIndex)
	}
}

// If not execute commands, server will directly reply based on commitLog function
func (r *Replica) commitLog(from int32, to int32) {
	dlog.Print("%d commiting log from %d to %d, with log size %d", r.Id, from, to, r.log.Size())
	for i := from + 1; i <= to; i++ {
		r.mu.Lock()
		req := r.pendingRequests[i]
		r.mu.Unlock()
		if req != nil && !r.Dreply {
			if req.Term == r.log.Get(i).Term && req.Index == i {
				// reply to client that their request was accepted (TRUE)
				propreply := &genericsmrproto.ProposeReplyTS{
					config.TRUE,
					req.Propose.CommandId,
					state.NIL,
					req.Propose.Timestamp,
				}
				r.ReplyProposeTS(propreply, req.Propose.Reply)
			} else {
				// reply to client that their request was not accepted (FALSE), maybe term mismatch due to another leader
				propreply := &genericsmrproto.ProposeReplyTS{
					config.FALSE,
					req.Propose.CommandId,
					state.NIL,
					req.Propose.Timestamp,
				}
				r.ReplyProposeTS(propreply, req.Propose.Reply)
			}
			r.mu.Lock()
			delete(r.pendingRequests, i)
			r.mu.Unlock()
		}
	}
	if !r.Dreply && config.Read_Local {
		r.proceedRead(false)
	}
	r.log.TruncateIfNeeded(to)
}

// If execute commands, server will reply after command executed
func (r *Replica) executeCommands() {
	for !r.Shutdown {
		applied := false

		for r.lastApplied < r.commitIndex && r.lastApplied+1 < r.log.Size() {
			dlog.Print("%d applied to %d -> %d, log is %d", r.Id, r.lastApplied, r.commitIndex, r.log.Size())
			r.lastApplied++
			idx := r.lastApplied
			entry := r.log.Get(idx)
			applied = true
			// Execute command on state machine
			val := entry.Command.Execute(r.State)
			r.mu.Lock()
			req, ok := r.pendingRequests[idx]
			r.mu.Unlock()
			// If this command came from a client, send the reply.
			if r.Dreply && ok {
				if req.Term == entry.Term {
					reply := &genericsmrproto.ProposeReplyTS{
						OK:        config.TRUE,
						CommandId: req.Propose.CommandId,
						Value:     val,
						Timestamp: req.Propose.Timestamp,
					}
					r.ReplyProposeTS(reply, req.Propose.Reply)
				} else {
					reply := &genericsmrproto.ProposeReplyTS{
						OK:        config.FALSE,
						CommandId: req.Propose.CommandId,
						Value:     state.NIL,
						Timestamp: req.Propose.Timestamp,
					}
					r.ReplyProposeTS(reply, req.Propose.Reply)
				}

				// Safe cleanup
				r.mu.Lock()
				delete(r.pendingRequests, idx)
				r.mu.Unlock()
			}
		}

		if r.Dreply && config.Read_Local {
			r.proceedRead(true)
		}
		if !applied {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (r *Replica) proceedRead(execute bool) {
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
				val = req.Propose.Command.Execute(r.State)
			} else {
				val = state.NIL
			}
			preply := &genericsmrproto.ProposeReplyTS{
				config.TRUE,
				req.Propose.CommandId,
				val,
				0,
			}
			r.ReplyProposeTS(preply, req.Propose.Reply)
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
