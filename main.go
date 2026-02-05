package main

import (
	"Mix/client"
	"Mix/config"
	"Mix/master"
	"Mix/server"
	"Mix/shard"
	"Mix/shardclient"
	"flag"
	"fmt"
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
		//shardclient.StartClient1()
	} else if config.CurrentInstance == config.ClientWithFail {
		shardclient.StartClient2()
	}
	//
	//else if config.CurrentInstance == config.ClientWithFail {
	//	//client.OneReqSharded()
	//	client.FiveReqSharded()
	//}
}

func hello() {
	shards := shard.NewShardInfo()
	for i := 0; i < 1000; i++ {
		key := shards.GetUnbalancedKey(int32(i))
		id, _ := shards.GetShardId(key)
		fmt.Printf("shard: %v ", id)
	}
}
