package client

import (
	"Mix/config"
	"Mix/genericsmrproto"
	"Mix/masterproto"
	"Mix/state"
	"bufio"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

func OneReq() {
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

	var leaderConn net.Conn
	var reader *bufio.Reader
	var writer *bufio.Writer
	var leaderAddr string

	// 3. Discover leader by sending a dummy request
	for index, replicaAddr := range rlReply.ReplicaList {
		conn, err := net.Dial("tcp", replicaAddr)
		if err != nil {
			fmt.Printf("Cannot connect to replica %s: %v\n", replicaAddr, err)
			continue
		}

		r := bufio.NewReader(conn)
		w := bufio.NewWriter(conn)

		// Send a dummy request to check if leader
		//var id int32 = 0
		args := genericsmrproto.Propose{
			CommandId: -1,
			Command: state.Command{
				Op: state.GET,
				K:  state.Key(0),
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
			fmt.Printf("Leader found at %s\n", replicaAddr)
			leaderConn = conn
			reader = r
			writer = w
			leaderAddr = replicaAddr
			break
		} else {
			fmt.Printf("Leader not found at  %d: %s\n", index, replicaAddr)
		}

		conn.Close() // not leader
	}

	if leaderAddr == "" {
		log.Fatalf("No leader found among replicas")
	}

	// 4. Send actual client request to leader
	var id int32 = 0
	args := genericsmrproto.Propose{
		CommandId: id,
		Command: state.Command{
			Op: state.PUT,
			K:  state.Key(123),
			V:  state.Value(999),
		},
		Timestamp: 0,
	}

	writer.WriteByte(genericsmrproto.PROPOSE)
	args.Marshal(writer)
	writer.Flush()

	fmt.Println("Sent request with CommandId =", id)

	reply := new(genericsmrproto.ProposeReplyTS)
	err = reply.Unmarshal(reader)
	if err != nil {
		log.Fatalf("Error reading reply from leader %s: %v\n", leaderAddr, err)
	}

	fmt.Printf("Received reply from leader %s: OK=%d Value=%d CommandId=%d\n",
		leaderAddr, reply.OK, reply.Value, reply.CommandId)

	leaderConn.Close()
}
