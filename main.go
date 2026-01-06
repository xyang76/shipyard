package main

import (
	"Mix/client"
	"Mix/config"
	"Mix/master"
	"Mix/server"
	"flag"
)

// Server

func main() {
	//TIP Press <shortcut actionId="ShowIntentionActions"/> when your caret is at the underlined or highlighted text
	// to see how GoLand suggests fixing it.
	flag.Parse()
	config.SetEnvironment()

	if config.CurrentInstance == config.Peer {

	} else if config.CurrentInstance == config.Master {
		master.Start()
	} else if config.CurrentInstance == config.Server {
		server.Start()
	} else if config.CurrentInstance == config.Client {
		if config.CurrentApproach == config.Raft ||
			config.CurrentApproach == config.Shipyard ||
			config.CurrentApproach == config.MultiRaft {
			client.StartRecoveryShardClient()
		} else {
			client.StartPaxosClient()
		}
	} else if config.CurrentInstance == config.ClientPerSec {
		//client.OneReq()
		client.StartRecoveryShardClientSec()
	} else if config.CurrentInstance == config.Test {
		//client.OneReqSharded()
		client.FiveReqSharded()
	}
}
