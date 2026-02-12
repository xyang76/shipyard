package main

import (
	"Mix/client"
	"Mix/clientfail"
	"Mix/config"
	"Mix/dlog"
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

	if config.CurrentInstance == config.Test {
		hello()
	} else if config.CurrentInstance == config.Master {
		master.Start()
	} else if config.CurrentInstance == config.Server {
		server.Start()
	} else if config.CurrentInstance == config.Client {
		client.StartRecoveryShardClient()
	} else if config.CurrentInstance == config.ClientPerSec {
		//client.OneReq()
		client.StartRecoveryShardClientSec()
		//clientsep.StartClient1()
	} else if config.CurrentInstance == config.ClientWithFail {
		//clientsep.StartClient2()
		clientfail.StartFailClient()
	}
	//
	//else if config.CurrentInstance == config.ClientWithFail {
	//	//client.OneReqSharded()
	//	client.FiveReqSharded()
	//}
}

func hello() {
	dlog.Info("Hello, welcome to shipyard!")
}
