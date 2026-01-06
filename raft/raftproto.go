package raft

import (
	"Mix/state"
)

const (
	REQUESTVOTE uint8 = iota
	REQUESTVOTE_REPLY
	APPENDENTRIES
	APPENDENTRIES_REPLY
)

type LogEntry struct {
	Command state.Command
	Term    int32
}

type RequestVoteArgs struct {
	Term         int32
	CandidateId  int32
	LastLogIndex int32
	LastLogTerm  int32

	Shard int32
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool

	Shard int32
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderId     int32
	PrevLogIndex int32

	PrevLogTerm int32
	//Entries      []state.Command
	Entries      []LogEntry
	LeaderCommit int32

	Shard int32
}

type AppendEntriesReply struct {
	Term    int32
	Success bool

	// Since we do not use RPC, and execute asynchronzely, we need the follower send back its id and last commit index
	FollowerId   int32
	LastLogIndex int32

	Shard int32
}
