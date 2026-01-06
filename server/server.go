package server

import (
	"Mix/base"
	"Mix/config"
	"Mix/dlog"
	"Mix/epaxos"
	"Mix/gpaxos"
	"Mix/masterproto"
	"Mix/mencius"
	"Mix/multiraft"
	"Mix/paxos"
	"Mix/raft"
	"Mix/shipyard"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"
)

var portnum *int = flag.Int("sport", 7070, "Port # to listen on. Defaults to 7070")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", false, "Execute commands.")
var dreply = flag.Bool("dreply", false, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")

func Start() {
	flag.Parse()

	runtime.GOMAXPROCS(*config.Procs)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt)
		go catchKill(interrupt)
	}

	log.Printf("Server starting on port %d\n", *portnum)

	replicaId, nodeList := registerWithMaster(fmt.Sprintf("%s:%d", *config.MasterAddr, *config.MasterPort))

	switch config.CurrentApproach {
	case config.Mencius:
		log.Println("Starting Mencius replica...")
		rep := mencius.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	case config.EPaxos:
		log.Println("Starting Egalitarian Paxos replica...")
		rep := epaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *beacon, *durable)
		rpc.Register(rep)
	case config.GPaxos:
		log.Println("Starting Generalized Paxos replica...")
		rep := gpaxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply)
		rpc.Register(rep)
	case config.Paxos:
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	case config.Raft:
		log.Println("Starting classic Raft replica...")
		rep := raft.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	case config.MultiRaft:
		log.Println("Starting multiple Raft replica...")
		rep := multiraft.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	case config.Shipyard:
		log.Println("Starting Shipyard replica...")
		rep := shipyard.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	case config.Base:
		log.Println("Starting Base replica...")
		rep := base.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	default:
		log.Println("Starting classic Paxos replica...")
		rep := paxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable)
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	//listen for RPC on a different port (8070 by default)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.Serve(l, nil)
}

func registerWithMaster(masterAddr string) (int, []string) {
	args := &masterproto.RegisterArgs{*myAddr, *portnum}
	var reply masterproto.RegisterReply

	for done := false; !done; {
		mcli, err := rpc.DialHTTP("tcp", masterAddr)
		if err == nil {
			err = mcli.Call("Master.Register", args, &reply)
			if err == nil && reply.Ready == true {
				done = true
				break
			}
			dlog.Info("Dialing master success")
		} else {
			dlog.Info("Dialing master failed:%v", err)
		}
		time.Sleep(1e9)
	}

	return reply.ReplicaId, reply.NodeList
}

func catchKill(interrupt chan os.Signal) {
	<-interrupt
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}
	fmt.Println("Caught signal")
	os.Exit(0)
}
