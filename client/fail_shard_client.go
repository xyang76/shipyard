package client

//
//import (
//	"Mix/config"
//	"Mix/genericsmrproto"
//	"Mix/masterproto"
//	"Mix/shard"
//	"Mix/state"
//	"bufio"
//	"flag"
//	"fmt"
//	"log"
//	"net"
//	"net/rpc"
//	"runtime"
//	"sync"
//	"sync/atomic"
//	"time"
//)

//
//func StartFailShardClient2() {
//	flag.Parse()
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
//	if len(rlReply.ReplicaList) == 0 {
//		log.Fatalf("No replicas returned by the master")
//	}
//
//	// --- 3. Initialize shard info ---
//	shardInfo := shard.NewShardInfo()
//
//	type LeaderConn struct {
//		conn   net.Conn
//		reader *bufio.Reader
//		writer *bufio.Writer
//		mu     sync.Mutex // to protect writer
//	}
//
//	shardLeaders := make(map[int32]*LeaderConn)
//	var shardMu sync.Mutex
//
//	// --- 4. Function to discover leader ---
//	findLeaderForShard := func(sid int32) *LeaderConn {
//		for _, replicaAddr := range rlReply.ReplicaList {
//			conn, err := net.Dial("tcp", replicaAddr)
//			if err != nil {
//				continue
//			}
//			r := bufio.NewReader(conn)
//			w := bufio.NewWriter(conn)
//
//			key := state.Key(sid)
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
//				conn.Close()
//				continue
//			}
//
//			if reply.OK != 0 {
//				return &LeaderConn{conn: conn, reader: r, writer: w}
//			} else {
//				conn.Close()
//			}
//		}
//		return nil
//	}
//
//	// --- 5. Prepare requests ---
//	type Request struct {
//		key state.Key
//		val state.Value
//		op  state.OpType
//	}
//
//	reqs := make([]Request, *reqsNum)
//	for i := 0; i < *reqsNum; i++ {
//		reqs[i] = Request{
//			key: state.Key(100 + i),
//			val: state.Value(i),
//			op:  state.PUT,
//		}
//	}
//
//	var successed int64
//	reqCh := make(chan Request, 1000)
//
//	// --- 6. Worker goroutines ---
//	numWorkers := 16
//	var wg sync.WaitGroup
//
//	for w := 0; w < numWorkers; w++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//			for req := range reqCh {
//				sid, _ := shardInfo.GetShardId(req.key)
//				var leader *LeaderConn
//
//				for {
//					shardMu.Lock()
//					leader = shardLeaders[sid]
//					shardMu.Unlock()
//
//					if leader == nil {
//						l := findLeaderForShard(sid)
//						if l == nil {
//							time.Sleep(50 * time.Millisecond)
//							continue
//						}
//						shardMu.Lock()
//						shardLeaders[sid] = l
//						leader = l
//						shardMu.Unlock()
//					}
//
//					args := genericsmrproto.Propose{
//						CommandId: int32(req.key),
//						Command: state.Command{
//							K: req.key,
//							V: req.val,
//							Op: req.op,
//						},
//						Timestamp: 0,
//					}
//
//					leader.mu.Lock()
//					leader.writer.WriteByte(genericsmrproto.PROPOSE)
//					args.Marshal(leader.writer)
//					err := leader.writer.Flush()
//					leader.mu.Unlock()
//
//					if err != nil {
//						leader.conn.Close()
//						shardMu.Lock()
//						delete(shardLeaders, sid)
//						shardMu.Unlock()
//						continue
//					}
//
//					reply := new(genericsmrproto.ProposeReplyTS)
//					err = reply.Unmarshal(leader.reader)
//					if err != nil ||
//						(reply.OK == config.FALSE &&
//							reply.CommandId == -1 &&
//							reply.Value == state.NIL &&
//							reply.Timestamp == 0) {
//						leader.conn.Close()
//						shardMu.Lock()
//						delete(shardLeaders, sid)
//						shardMu.Unlock()
//						continue
//					}
//
//					atomic.AddInt64(&successed, 1)
//					break
//				}
//			}
//		}()
//	}
//
//	// --- 7. Feed requests to workers ---
//	go func() {
//		for _, r := range reqs {
//			reqCh <- r
//		}
//		close(reqCh)
//	}()
//
//	// --- 8. Periodic success reporting ---
//	ticker := time.NewTicker(2 * time.Second)
//	done := make(chan struct{})
//	go func() {
//		for {
//			select {
//			case <-ticker.C:
//				fmt.Printf("Success so far: %d\n", atomic.LoadInt64(&successed))
//			case <-done:
//				return
//			}
//		}
//	}()
//
//	startTime := time.Now()
//	wg.Wait()
//	ticker.Stop()
//	close(done)
//	endTime := time.Now()
//
//	fmt.Printf("Sent %d requests, received %d successful replies\n", *reqsNum, atomic.LoadInt64(&successed))
//	fmt.Printf("Total time: %v\n", endTime.Sub(startTime))
//
//	// --- 9. Close all shard connections ---
//	shardMu.Lock()
//	for _, l := range shardLeaders {
//		l.conn.Close()
//	}
//	shardMu.Unlock()
//}
