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

func NewFinishNotify(round int, numReqs int, shards *shard.ShardInfo) *FinishNotify {
	done := make([]chan struct{}, round)
	for i := 0; i < round; i++ {
		done[i] = make(chan struct{}) // close-only
	}
	return &FinishNotify{
		replyTimes: make([]int64, round),
		roundArr:   make([]int64, round),
		done:       done,
		round:      round,
		numReqs:    numReqs,
		shards:     shards,
	}
}

func (n *FinishNotify) notifyConnectFail(err error) {
	dlog.Info("connect fail %v", err)
}

func (n *FinishNotify) notifyInvalidLeader(rid int, reply *genericsmrproto.ProposeReplyTS, err error) {
	dlog.Info("Received %v InvalidLeader reply %v of replica: err %v", rid, reply, err)
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
	dlog.Info("Received Command skip")

	r := int(reqID) / n.numReqs
	n.mu.Lock()
	n.skipped++
	n.roundArr[r]++
	arrivals := n.roundArr[r]
	n.mu.Unlock()

	if arrivals == int64(n.numReqs) {
		n.signalDone(r)
	}
}

func (n *FinishNotify) notifyCommandFail(cmd *genericsmrproto.Propose, err error) {
	dlog.Info("Received Command fail %v", err)
	n.mu.Lock()
	n.failed++
	n.mu.Unlock()
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
