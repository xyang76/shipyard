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
var HeartBeatInterval *int = flag.Int("hi", 1000, "Heartbeat interval in milliseconds")
var HeartBeatTimeout *int = flag.Int("ht", 10000, "Heartbeat timeout in milliseconds")
var RaftElectionInterval *int = flag.Int("et", 1000, "election timeout in milliseconds")
var BalanceRegenerate *int = flag.Int("to", 500, "Token regeneration in milliseconds")
var ReplyReceiveTimeout *int = flag.Int("rrt", 10000, "Since leader may crash, lost connection, we need this timeout to count elapse")
var BatchSize *int = flag.Int("mb", 10, "max batch size")
var AutoBalance *int = flag.Int("ab", 1, "auto balance")

const CHAN_BUFFER_SIZE = 5000
const LOG_SIZE = 512 * 1024
const PAXOS_LOG_SIZE = 1024 * 1024
const TRUE = uint8(1)
const FALSE = uint8(0)

var TickTime *int = flag.Int("tt", 5, "Tick time in milliseconds")
var FastRaft *int = flag.Int("cmt", 0, "Tick time in milliseconds")

var MAX_BATCH = 2
var Auto_Balance = true

const Max_CMD_SECOND = 50000 //
const Read_Local = true      //For raft/shipyard, we do not need replicate reads
const Fail_Prone = true      // Need reconnection
const Write_Log = true
const PrintApportion = false

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
	MAX_BATCH = *BatchSize
	Auto_Balance = *AutoBalance != 0
}

// electionTimeout generates a pseudo-random election timeout duration.
func RandomElectionTimeout() time.Duration {
	min := *HeartBeatTimeout
	max := *HeartBeatTimeout * 2
	return time.Duration(min+rand.Intn(max-min)) * time.Millisecond
}
