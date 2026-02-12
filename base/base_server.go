package base

import (
	"Mix/config"
	"Mix/dlog"
	"Mix/genericsmr"
	"Mix/genericsmrproto"
	"Mix/state"
	"time"
)

type Replica struct {
	*genericsmr.Replica
	Shutdown bool
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
	r := &Replica{
		Replica:  genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
		Shutdown: false,
	}

	go r.run()
	return r
}

func (r *Replica) run() {
	r.ConnectToPeers()

	dlog.Println("Waiting for client connections")

	go r.WaitForClientConnections()

	tick := time.NewTicker(30 * time.Millisecond)
	defer tick.Stop()

	dlog.Println("Base replica running, waiting for client proposes")
	onOffProposeChan := r.ProposeChan

	for !r.Shutdown {
		select {
		case <-tick.C:
			onOffProposeChan = r.ProposeChan
			break

		case prop := <-onOffProposeChan:
			// got a client propose
			//dlog.Info("Base replica got proposal Op=%d Id=%d", prop.Command.Op, prop.CommandId)
			r.handlePropose(prop)
			// after receiving a proposal, optionally pause accepting more to prioritize internal messages
			onOffProposeChan = nil
			break
		}
	}
}

func (r *Replica) handlePropose(propose *genericsmr.Propose) {
	dlog.Print("%d received propose %v\n", r.Id, propose.Command)

	preply := &genericsmrproto.ProposeReplyTS{config.TRUE, propose.CommandId, state.NIL, 0}
	r.ReplyProposeTS(preply, propose.Reply)
}
