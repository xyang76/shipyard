package clientfail

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmrproto"
	"Mix/shard"
	"fmt"
	"sync"
	"time"
)

type FinishNotify struct {
	base    *BaseClient
	success int64
	failed  int64
	skipped int64

	round   int
	numReqs int

	replyTimes []int64 // per round completion time
	roundArr   []int64 // total arrivals per round
	done       []chan struct{}
	shards     *shard.ShardInfo
	mu         sync.Mutex
}

func NewFinishNotify(base *BaseClient, round int, numReqs int) *FinishNotify {
	done := make([]chan struct{}, round)
	for i := 0; i < round; i++ {
		done[i] = make(chan struct{}) // close-only
	}
	return &FinishNotify{
		base:       base,
		replyTimes: make([]int64, round),
		roundArr:   make([]int64, round),
		done:       done,
		round:      round,
		numReqs:    numReqs,
		shards:     base.shards,
	}
}

func (n *FinishNotify) notifyConnectFail(rid int, err error) {
	dlog.Info("connect fail (replica:%v) %v", rid, err)
	n.base.removeLeader(rid)
}

func (n *FinishNotify) notifyInvalidLeader(rid int, reply *genericsmrproto.ProposeReplyTS, err error) {
	dlog.Info("Rid:%v received invalidLeader reply %v, sid %v of err %v", rid, reply, reply.Timestamp, err)
	//shard := reply.Timestamp
}

func (n *FinishNotify) notifyCommandFinish(reply *genericsmrproto.ProposeReplyTS) {
	dlog.Print("Received Command reply %v", reply)

	n.mu.Lock()
	round := int(reply.CommandId) / n.numReqs
	n.replyTimes[round] = time.Now().UnixNano()
	n.roundArr[round]++
	if reply.OK == config.TRUE {
		n.success++
	} else {
		n.failed++
	}
	arrivals := n.roundArr[round]
	n.mu.Unlock()

	if arrivals == int64(n.numReqs) {
		n.signalDone(round)
	}
}

func (n *FinishNotify) notifyCommandSkip(reqID int32) {
	dlog.Print("Received Command skip %v", reqID)

	r := int(reqID) / n.numReqs
	n.mu.Lock()
	n.skipped++
	n.roundArr[r]++
	arrivals := n.roundArr[r]
	n.mu.Unlock()
	time.Sleep(1 * time.Millisecond)
	if arrivals == int64(n.numReqs) {
		n.signalDone(r)
	}
}

func (n *FinishNotify) notifyCommandFail(cmd *genericsmrproto.Propose, err error) {
	dlog.Info("Received Command fail %v", err)
	n.mu.Lock()
	r := int(cmd.CommandId) / n.numReqs
	n.failed++
	n.roundArr[r]++
	arrivals := n.roundArr[r]
	n.mu.Unlock()
	if arrivals == int64(n.numReqs) {
		n.signalDone(r)
	}
}

// minimal helper to avoid double-close
func (n *FinishNotify) signalDone(r int) {
	select {
	case <-n.done[r]:
		// already closed
	default:
		close(n.done[r])
	}
}

func (n *FinishNotify) printInfo() {
	fmt.Printf("Success so far: %d, skipped:%v, failed:%v\n",
		n.success, n.skipped, n.failed)
}

func (n *FinishNotify) notifyLeaderReset(idx int, shard int32) {
	dlog.Info("Received leader reset %v of shard %v", idx, shard)
}
