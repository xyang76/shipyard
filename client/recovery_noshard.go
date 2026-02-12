package client

import (
	"Mix/config"
	"Mix/masterproto"
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"runtime"
)

func StartRecoveryNoShardClient() {
	flag.Parse()

	runtime.GOMAXPROCS(runtime.NumCPU())

	if *config.Conflicts > 100 {
		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
	}

	// --- 1. Connect to master ---
	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *config.MasterAddr, *config.MasterPort))
	if err != nil {
		log.Fatalf("Cannot connect to master: %v\n", err)
	}
	defer master.Close()

	// --- 2. Get replica list ---
	rl := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rl)
	if err != nil {
		log.Fatalf("GetReplicaList failed: %v\n", err)
	}

	if len(rl.ReplicaList) == 0 {
		log.Fatalf("No replicas returned by the master")
	}

	//round := *config.Rounds
	//writePercent := *config.Writes
	//reqID := int32(0)
	//last := int64(0)

}
