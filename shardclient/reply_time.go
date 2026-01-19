package shardclient

import (
	"Mix/config"
	"Mix/genericsmrproto"
	"sync/atomic"
	"time"
)

type ReplyTime struct {
	round   int
	numReqs int

	replyTimes []int64 // per round completion time
	roundArr   []int64 // total arrivals per round

	send    map[int32]int64
	skipped map[int32]int64

	success   int64
	failed    int64
	lastRound int64
}

// NewReplyTime creates a per-shard, per-round ReplyTime
func NewReplyTime(round int, numReqs int) *ReplyTime {
	return &ReplyTime{
		round:      round,
		numReqs:    numReqs,
		replyTimes: make([]int64, round*10),
		roundArr:   make([]int64, round*10),
		send:       make(map[int32]int64),
		skipped:    make(map[int32]int64),
	}
}

// OnReplyArrival records a message arrival and notifies the external WaitGroup
func (rt *ReplyTime) OnReplyArrival(reply *genericsmrproto.ProposeReplyTS) {
	round := int(reply.CommandId) / rt.numReqs
	atomic.StoreInt64(&rt.replyTimes[round], time.Now().UnixNano())
	atomic.AddInt64(&rt.roundArr[round], 1)
	if reply.OK == config.TRUE {
		atomic.AddInt64(&rt.success, 1)
	} else {
		atomic.AddInt64(&rt.failed, 1)
	}

}
