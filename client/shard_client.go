package client

/** This should be used by recovery_shard instead **/
//
//import (
//	"shipyard/config"
//	"shipyard/genericsmrproto"
//	"shipyard/masterproto"
//	"shipyard/shard"
//	"shipyard/state"
//	"bufio"
//	"flag"
//	"fmt"
//	"log"
//	"net"
//	"net/rpc"
//	"runtime"
//	"time"
//)
//
//func StartShardClient() {
//	flag.Parse()
//
//	runtime.GOMAXPROCS(runtime.NumCPU())
//
//	if *conflicts > 100 {
//		log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
//	}
//
//	// --- 1. Connect to master ---
//	master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *config.MasterAddr, *config.MasterPort))
//	if err != nil {
//		log.Fatalf("Cannot connect to master: %v\n", err)
//	}
//	defer master.Close()
//
//	// --- 2. Get replica list ---
//	rlReply := new(masterproto.GetReplicaListReply)
//	err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
//	if err != nil {
//		log.Fatalf("GetReplicaList failed: %v\n", err)
//	}
//
//	if len(rlReply.ReplicaList) == 0 {
//		log.Fatalf("No replicas returned by the master")
//	}
//
//	// 3. Initialize shard info
//	shardInfo := shard.NewShardInfo()
//	shardLeaders := make(map[int32]struct {
//		conn   net.Conn
//		reader *bufio.Reader
//		writer *bufio.Writer
//	})
//
//	// 4. Discover leader for each shard
//	for _, sid := range shardInfo.Shards {
//		for _, replicaAddr := range rlReply.ReplicaList {
//			conn, err := net.Dial("tcp", replicaAddr)
//			if err != nil {
//				fmt.Printf("Cannot connect to replica %s: %v\n", replicaAddr, err)
//				continue
//			}
//
//			r := bufio.NewReader(conn)
//			w := bufio.NewWriter(conn)
//
//			// Send IdentifyLeader request for this shard
//			// Use a dummy key that maps to the shard
//			key := state.Key(sid) // simple choice; modulo logic ensures correct shard
//			args := genericsmrproto.Propose{
//				CommandId: config.IdentifyLeader,
//				Command: state.Command{
//					Op: state.GET,
//					K:  key,
//					V:  state.Value(0),
//				},
//				Timestamp: 0,
//			}
//
//			w.WriteByte(genericsmrproto.PROPOSE)
//			args.Marshal(w)
//			w.Flush()
//
//			reply := new(genericsmrproto.ProposeReplyTS)
//			err = reply.Unmarshal(r)
//			if err != nil {
//				fmt.Printf("Error reading reply from %s: %v\n", replicaAddr, err)
//				conn.Close()
//				continue
//			}
//
//			if reply.OK != 0 {
//				fmt.Printf("Leader for shard %d found at %s\n", sid, replicaAddr)
//				shardLeaders[sid] = struct {
//					conn   net.Conn
//					reader *bufio.Reader
//					writer *bufio.Writer
//				}{conn, r, w}
//				break
//			} else {
//				conn.Close()
//			}
//		}
//
//		if _, ok := shardLeaders[sid]; !ok {
//			fmt.Printf("No leader found for shard %d\n", sid)
//		}
//	}
//
//	// --- 5. Send requests to shard leaders ---
//	reqsNb := *ReqsNum // total number of requests
//
//	keysPerRequest := make([]state.Key, reqsNb)
//	put := make([]bool, reqsNb)
//
//	for i := 0; i < reqsNb; i++ {
//		keysPerRequest[i] = state.Key(100 + i) // example keys
//		put[i] = true                          // all PUT, you can randomize if needed
//	}
//	//
//	successed := 0
//	startTime := time.Now()
//
//	for i := 0; i < reqsNb; i++ {
//		key := keysPerRequest[i]
//		sid, _ := shardInfo.GetShardId(key)
//		leader, ok := shardLeaders[sid]
//		if !ok {
//			fmt.Printf("Skipping key %d, no leader for shard %d\n", key, sid)
//			continue
//		}
//
//		args := genericsmrproto.Propose{
//			CommandId: int32(i),
//			Command: state.Command{
//				K: key,
//				V: state.Value(i), // example value
//			},
//			Timestamp: 0,
//		}
//
//		if put[i] {
//			args.Command.Op = state.PUT
//		} else {
//			args.Command.Op = state.GET
//		}
//
//		leader.writer.WriteByte(genericsmrproto.PROPOSE)
//		args.Marshal(leader.writer)
//		if i%100 == 0 { // flush periodically
//			leader.writer.Flush()
//		}
//	}
//
//	for _, l := range shardLeaders {
//		l.writer.Flush() // flush remaining requests
//	}
//
//	// Read replies
//	for i := 0; i < reqsNb; i++ {
//		key := keysPerRequest[i]
//		sid, _ := shardInfo.GetShardId(key)
//		leader, ok := shardLeaders[sid]
//		if !ok {
//			continue
//		}
//
//		reply := new(genericsmrproto.ProposeReplyTS)
//		err := reply.Unmarshal(leader.reader)
//		if err != nil {
//			fmt.Printf("Error reading reply for key %d from shard %d leader: %v\n", key, sid, err)
//			continue
//		}
//
//		successed++
//	}
//	endTime := time.Now()
//	fmt.Printf("Sent %d requests, received %d successed replies\n", reqsNb, successed)
//	fmt.Printf("Total time: %v\n", endTime.Sub(startTime))
//
//	// --- 6. Close all shard leader connections ---
//	for _, l := range shardLeaders {
//		l.conn.Close()
//	}
//
//}
