package client

import (
	"Mix/config"
	"Mix/genericsmrproto"
	"Mix/masterproto"
	"Mix/shard"
	"Mix/state"
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

func FiveReqSharded() {
	// 1. Connect to master
	master, err := rpc.DialHTTP("tcp",
		fmt.Sprintf("%s:%d", *config.MasterAddr, *config.MasterPort))
	if err != nil {
		log.Fatalf("Cannot connect to master: %v\n", err)
	}
	defer master.Close()

	// 2. Get replica list
	rlReply := new(masterproto.GetReplicaListReply)
	err = master.Call("Master.GetReplicaList",
		new(masterproto.GetReplicaListArgs), rlReply)
	if err != nil {
		log.Fatalf("GetReplicaList failed: %v\n", err)
	}

	if len(rlReply.ReplicaList) == 0 {
		log.Fatalf("No replicas returned by the master")
	}

	// 3. Initialize shard info
	shardInfo := shard.NewShardInfo()
	shardLeaders := make(map[int32]struct {
		conn   net.Conn
		reader *bufio.Reader
		writer *bufio.Writer
	})

	keys := []state.Key{123, 456, 789}

	// 4. Discover leader for each shard
	for _, sid := range shardInfo.Shards {
		for _, replicaAddr := range rlReply.ReplicaList {
			conn, err := net.Dial("tcp", replicaAddr)
			if err != nil {
				fmt.Printf("Cannot connect to replica %s: %v\n", replicaAddr, err)
				continue
			}

			r := bufio.NewReader(conn)
			w := bufio.NewWriter(conn)

			// Send IdentifyLeader request for this shard
			// Use a dummy key that maps to the shard
			key := state.Key(sid) // simple choice; modulo logic ensures correct shard
			args := genericsmrproto.Propose{
				CommandId: config.IdentifyLeader,
				Command: state.Command{
					Op: state.GET,
					K:  key,
					V:  state.Value(0),
				},
				Timestamp: 0,
			}

			w.WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(w)
			w.Flush()

			reply := new(genericsmrproto.ProposeReplyTS)
			err = reply.Unmarshal(r)
			if err != nil {
				fmt.Printf("Error reading reply from %s: %v\n", replicaAddr, err)
				conn.Close()
				continue
			}

			if reply.OK != 0 {
				fmt.Printf("Leader for shard %d found at %s\n", sid, replicaAddr)
				shardLeaders[sid] = struct {
					conn   net.Conn
					reader *bufio.Reader
					writer *bufio.Writer
				}{conn, r, w}
				break
			} else {
				conn.Close()
			}
		}

		if _, ok := shardLeaders[sid]; !ok {
			fmt.Printf("No leader found for shard %d\n", sid)
		}
	}

	// 5. Send requests to each shard leader
	// 5. Send 5 requests to each shard leader
	for _, key := range keys {
		sid, _ := shardInfo.GetShardId(key)
		leader, ok := shardLeaders[sid]
		if !ok {
			fmt.Printf("Skipping key %d, no leader for shard %d\n", key, sid)
			continue
		}

		for i := 0; i < 5; i++ {
			var id int32 = int32(i) // unique per request (per shard)

			args := genericsmrproto.Propose{
				CommandId: id,
				Command: state.Command{
					Op: state.PUT,
					K:  key,
					V:  state.Value(1000 + i), // distinguish each request
				},
				Timestamp: 0,
			}

			leader.writer.WriteByte(genericsmrproto.PROPOSE)
			args.Marshal(leader.writer)
			leader.writer.Flush()

			reply := new(genericsmrproto.ProposeReplyTS)
			err := reply.Unmarshal(leader.reader)
			if err != nil {
				fmt.Printf("Error reading reply from shard %d leader: %v\n", sid, err)
				break
			}

			fmt.Printf(
				"Shard %d req %d: OK=%d Value=%d CommandId=%d\n",
				sid, i, reply.OK, reply.Value, reply.CommandId,
			)
		}
	}

	// 6. Close all leader connections
	for _, l := range shardLeaders {
		l.conn.Close()
	}
}
