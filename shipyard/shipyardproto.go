package shipyard

import "Mix/state"

type LogEntry struct {
	Command   state.Command
	Epoch     int32
	Apportion int32
}

type HeartbeatArgs struct {
	Shards    []int32
	Apportion int32
	Sender    int32
}

// HeartbeatReply Since we are not using RPC here, so reply is not necessary
type HeartbeatReply struct {
	OK bool
}

type VoteAndGatherArgs struct {
	Epoch     int32
	Apportion int32
	Shard     int32

	CandidateId     int32
	CandidateCommit int32
}

type VoteAndGatherReply struct {
	Epoch     int32
	Apportion int32
	Shard     int32
	PeerId    int32

	PeerCommit int32
	Diff       []LogEntry
	OK         bool
}

type AppendEntriesArgs struct {
	Epoch         int32
	Apportion     int32
	CurrentStatus int32

	LeaderCommit int32
	StartIndex   int32

	Diff     []LogEntry
	LeaderId int32

	Shard int32
}

type AppendEntriesReply struct {
	Epoch       int32
	Apportion   int32
	CommitIndex int32

	OK         bool
	FollowerId int32
	Shard      int32
}

type BalanceArgs struct {
	LeaderId          int32
	Sender            int32
	ProposalApportion int32
	Shard             int32
}

type BalanceReply struct {
	Token bool
	Shard int32
}
