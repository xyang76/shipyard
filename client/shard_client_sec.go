package client

/** This should be used by recovery_shard_sec instead **/
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
//	"sync/atomic"
//	"time"
//)
//
//func StartShardClientSec() {
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
//	// --- 5. Periodic send + async receive ---
//	var successed int64
//	var reqID int32 = 0
//
//	// 5.1 Start async reply readers (one per shard leader)
//	for sid, leader := range shardLeaders {
//		go func(sid int32, l struct {
//			conn   net.Conn
//			reader *bufio.Reader
//			writer *bufio.Writer
//		}) {
//			reply := new(genericsmrproto.ProposeReplyTS)
//			for {
//				if err := reply.Unmarshal(l.reader); err != nil {
//					// connection closed or leader failed
//					return
//				}
//				if reply.OK != 0 {
//					atomic.AddInt64(&successed, 1)
//				}
//			}
//		}(sid, leader)
//	}
//
//	// 5.2 Periodic success reporting
//	reportTicker := time.NewTicker(2 * time.Second)
//	defer reportTicker.Stop()
//
//	go func() {
//		for range reportTicker.C {
//			fmt.Printf("Success so far: %d\n", atomic.LoadInt64(&successed))
//		}
//	}()
//
//	// 5.3 Periodic sending loop
//	sendTicker := time.NewTicker(1 * time.Second)
//	defer sendTicker.Stop()
//
//	batchSize := *ReqsNum // per second
//	turns := 100          // e.g., number of seconds to run
//
//	startTime := time.Now()
//
//	for t := 0; t < turns; t++ {
//		<-sendTicker.C
//
//		for i := 0; i < batchSize; i++ {
//			key := state.Key(100 + int(reqID))
//			sid, _ := shardInfo.GetShardId(key)
//
//			leader, ok := shardLeaders[sid]
//			if !ok {
//				continue
//			}
//
//			args := genericsmrproto.Propose{
//				CommandId: reqID,
//				Command: state.Command{
//					Op: state.PUT,
//					K:  key,
//					V:  state.Value(reqID),
//				},
//				Timestamp: 0,
//			}
//
//			leader.writer.WriteByte(genericsmrproto.PROPOSE)
//			args.Marshal(leader.writer)
//
//			// batch flush
//			if reqID%100 == 0 {
//				leader.writer.Flush()
//			}
//
//			reqID++
//		}
//
//		// flush all leaders after each second
//		for _, l := range shardLeaders {
//			l.writer.Flush()
//		}
//	}
//
//	elapsed := time.Since(startTime)
//	fmt.Printf("Finished sending. Success=%d, time=%v\n",
//		atomic.LoadInt64(&successed), elapsed)
//
//	// --- 6. Close all shard leader connections ---
//	for _, l := range shardLeaders {
//		l.conn.Close()
//	}
//}
