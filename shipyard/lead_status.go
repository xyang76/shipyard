package shipyard

import (
	"fmt"
	"time"
)

var StateInterval = 3
var StateStart = 0

type LeadState struct {
	rep      *Replica
	interval int
	count    int
	start    int
	id       int32
}

func NewLeadState(rep *Replica) *LeadState {
	return &LeadState{
		id:       rep.Id,
		rep:      rep,
		count:    0,
		start:    StateStart,
		interval: StateInterval,
	}
}

func (l *LeadState) StartTimer() {
	ticker := time.NewTicker(time.Duration(l.interval) * time.Second)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				// Execute the function
				l.executeTimer()
			case <-stop:
				// Stop the ticker
				ticker.Stop()
				return
			}
		}
	}()
}

func (l *LeadState) executeTimer() {
	shards := l.rep.leadingShards
	//info := l.env.ShardStatus()
	l.count++
	format := ""
	now := time.Now()
	hours := now.Hour()
	seconds := now.Second()
	minutes := now.Minute()
	for i := 0; i < len(shards); i++ {
		shard := string('A' + shards[i])
		server := fmt.Sprintf("S%d", l.id)
		format = format + fmt.Sprintf("(%v, %v, %v, %v)", hours, minutes*60+seconds, shard, server)
	}
	//fmt.Println(l.id + "--" + info)
	fmt.Println(format)
}
