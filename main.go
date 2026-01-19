package main

import (
	"Mix/client"
	"Mix/config"
	"Mix/master"
	"Mix/server"
	"Mix/shardclient"
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
		client.StartRecoveryShardClient()
	} else if config.CurrentInstance == config.ClientPerSec {
		//client.OneReq()
		//client.StartRecoveryShardClientSec()
		shardclient.StartClient1()
	} else if config.CurrentInstance == config.Test {
		shardclient.StartClient2()
	}
	//
	//else if config.CurrentInstance == config.Test {
	//	//client.OneReqSharded()
	//	client.FiveReqSharded()
	//}
}
