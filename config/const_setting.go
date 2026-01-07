package config

import (
	"flag"
	"math/rand"
	"time"
)

// 1: Master, 2: server, 3: client
var role *int = flag.Int("r", 2, "The role to start. 0: Via config 1: Master, 2: server, 3: client")

// 0: Paxos, 1:Mencius, 2: GPaxos, 3:EPaxos, 4:Raft, 5: MultiRaft, 6: Shipyard
//var app *int = flag.Int("a", 0, "Approach. 0: Paxos, 1:Mencius, 2: GPaxos, 3:EPaxos, 4:Raft, 5: MultiRaft, 6: Shipyard")

var app *int = flag.Int("a", 5, "Approach. 0: Paxos, 1:Mencius, 2: GPaxos, 3:EPaxos, 4:Raft, 5: MultiRaft, 6: Shipyard")

// Master
var MasterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var MasterPort *int = flag.Int("mport", 7087, "Master port.  Defaults to 7087.")
var NumNodes *int = flag.Int("N", 5, "Number of replicas. Defaults to 3.")

// Server
var Procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")

// Shards
var ShardNum *int = flag.Int("s", 9, "Number of shards. Only used for multi-raft and shipyard.")
var ShardStatus *bool = flag.Bool("status", false, "Show shard leading status.")
var SeperateClientPort *bool = flag.Bool("sp", true, "Seperate client ports and peer ports.")

// Raft and Shipyard
var HeartBeatInterval *int = flag.Int("hi", 2000, "Heartbeat interval in milliseconds")
var HeartBeatTimeout *int = flag.Int("ht", 10000, "Heartbeat timeout in milliseconds")
var BalanceInterval *int = flag.Int("bi", 2000, "Balance interval in milliseconds")
var TokenRegenerate *int = flag.Int("to", 5000, "Token regeneration in milliseconds")
var ReplyReceiveTimeout *int = flag.Int("rrt", 5000, "Since leader may crash, lost connection, we need this timeout to count elapse")

const CHAN_BUFFER_SIZE = 500000
const LOG_SIZE = 512 * 1024
const PAXOS_LOG_SIZE = 1024 * 1024
const TRUE = uint8(1)
const FALSE = uint8(0)

var TickTime *int = flag.Int("tt", 5, "Tick time in milliseconds")
var Commit *int = flag.Int("cmt", 1, "Tick time in milliseconds")

const MAX_BATCH = 10
const Max_CMD_SECOND = 50000 //
const Read_Local = true      //For raft/shipyard, we do not need replicate reads
const Fail_Prone = false     // Need reconnection
const Write_Log = true

type Approach int

const (
	Paxos Approach = iota
	Mencius
	GPaxos
	EPaxos
	Raft
	MultiRaft
	Shipyard
	Base
)

type Instance int

const (
	Peer Instance = iota
	Master
	Server
	Client
	ClientPerSec
	Test
)

var CurrentInstance Instance
var CurrentApproach Approach

func SetEnvironment() {
	CurrentInstance = Instance(*role)
	CurrentApproach = Approach(*app)
}

// electionTimeout generates a pseudo-random election timeout duration.
func RandomElectionTimeout() time.Duration {
	min := *HeartBeatTimeout
	max := *HeartBeatTimeout * 2
	return time.Duration(min+rand.Intn(max-min)) * time.Millisecond
}
