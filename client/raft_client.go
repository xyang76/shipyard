package client

//
//import (
//	"Mix/config"
//	"Mix/genericsmrproto"
//	"Mix/masterproto"
//	"Mix/state"
//	"bufio"
//	"flag"
//	"fmt"
//	"log"
//	"math/rand"
//	"net"
//	"net/rpc"
//	"runtime"
//	"time"
//)
//
//// This start Raft, Shipyard clients
//func StartRaftClient() {
//	flag.Parse()
//
//	runtime.GOMAXPROCS(runtime.NumCPU())
//
//	randObj := rand.New(rand.NewSource(42))
//	zipf := rand.NewZipf(randObj, *s, *v, uint64(*reqsNum / *rounds + *eps))
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
//	// --- 3. Discover leader by sending a dummy request ---
//	var leaderConn net.Conn
//	var reader *bufio.Reader
//	var writer *bufio.Writer
//	var leaderAddr string
//
//	for index, replicaAddr := range rlReply.ReplicaList {
//		conn, err := net.Dial("tcp", replicaAddr)
//		if err != nil {
//			fmt.Printf("Cannot connect to replica %s: %v\n", replicaAddr, err)
//			continue
//		}
//		r := bufio.NewReader(conn)
//		w := bufio.NewWriter(conn)
//
//		// Dummy request to detect leader
//		args := genericsmrproto.Propose{
//			CommandId: config.IdentifyLeader,
//			Command: state.Command{
//				Op: state.GET,
//				K:  state.Key(0),
//				V:  state.Value(0),
//			},
//			Timestamp: 0,
//		}
//		w.WriteByte(genericsmrproto.PROPOSE)
//		args.Marshal(w)
//		w.Flush()
//
//		reply := new(genericsmrproto.ProposeReplyTS)
//		err = reply.Unmarshal(r)
//		if err != nil {
//			fmt.Printf("Error reading reply from %s: %v\n", replicaAddr, err)
//			conn.Close()
//			continue
//		}
//
//		if reply.OK != 0 {
//			fmt.Printf("Leader found at %s\n", replicaAddr)
//			leaderConn = conn
//			reader = r
//			writer = w
//			leaderAddr = replicaAddr
//			break
//		} else {
//			fmt.Printf("Leader not found at %d: %s\n", index, replicaAddr)
//			conn.Close()
//		}
//	}
//
//	if leaderAddr == "" {
//		log.Fatalf("No leader found among replicas")
//	}
//
//	// --- Prepare Raft request arrays ---
//	N = len(rlReply.ReplicaList)
//	rarray = make([]int, *reqsNum / *rounds + *eps)
//	karray := make([]int64, *reqsNum / *rounds + *eps)
//	put := make([]bool, *reqsNum / *rounds + *eps)
//	perReplicaCount := make([]int, N)
//	test := make([]int, *reqsNum / *rounds + *eps)
//
//	for i := 0; i < len(rarray); i++ {
//		r := randObj.Intn(N)
//		rarray[i] = r
//		if i < *reqsNum / *rounds {
//			perReplicaCount[r]++
//		}
//
//		if *conflicts >= 0 {
//			if randObj.Intn(100) < *conflicts {
//				karray[i] = 42
//			} else {
//				karray[i] = int64(43 + i)
//			}
//			put[i] = randObj.Intn(100) < *writes
//		} else {
//			karray[i] = int64(zipf.Uint64())
//			test[karray[i]]++
//		}
//	}
//
//	if *conflicts >= 0 {
//		fmt.Println("Uniform distribution")
//	} else {
//		fmt.Println("Zipfian distribution")
//	}
//
//	successful = make([]int, N)
//	var id int32 = 0
//	done := make(chan bool, N)
//	beforeTotal := time.Now()
//
//	// --- Run rounds ---
//	for j := 0; j < *rounds; j++ {
//		n := *reqsNum / *rounds
//
//		if *check {
//			rsp = make([]bool, n)
//			for j := 0; j < n; j++ {
//				rsp[j] = false
//			}
//		}
//
//		go waitReplies([]*bufio.Reader{reader}, 0, n, done)
//
//		before := time.Now()
//		for i := 0; i < n+*eps; i++ {
//			args := genericsmrproto.Propose{
//				CommandId: id,
//				Command: state.Command{
//					K: state.Key(karray[i]),
//					V: state.Value(i),
//				},
//				Timestamp: 0,
//			}
//			if put[i] {
//				args.Command.Op = state.PUT
//			} else {
//				args.Command.Op = state.GET
//			}
//			writer.WriteByte(genericsmrproto.PROPOSE)
//			args.Marshal(writer)
//			if i%100 == 0 {
//				writer.Flush()
//			}
//			id++
//		}
//		writer.Flush()
//
//		err := <-done
//		after := time.Now()
//		fmt.Printf("Round %d took %v\n", j, after.Sub(before))
//
//		if *check {
//			for j := 0; j < n; j++ {
//				if !rsp[j] {
//					fmt.Println("Didn't receive reply for CommandId", j)
//				}
//			}
//		}
//
//		if err {
//			log.Println("Error detected during replies; leader might have changed")
//		}
//	}
//
//	afterTotal := time.Now()
//	fmt.Printf("Test took %v\n", afterTotal.Sub(beforeTotal))
//
//	totalSucc := 0
//	for _, s := range successful {
//		totalSucc += s
//	}
//	fmt.Printf("Successful replies: %d\n", totalSucc)
//
//	leaderConn.Close()
//}
