package config

import (
	"flag"
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
var ShardNum *int = flag.Int("s", 3, "Number of shards. Only used for multi-raft and shipyard.")
var ShardStatus *bool = flag.Bool("status", false, "Show shard leading status.")
var SeperateClientPort *bool = flag.Bool("sp", true, "Seperate client ports and peer ports.")

// Raft and Shipyard
var HeartBeatInterval *int = flag.Int("hi", 1000, "Heartbeat interval in milliseconds")
var HeartBeatTimeout *int = flag.Int("ht", 3000, "Heartbeat timeout in milliseconds")
var RaftElectionInterval *int = flag.Int("et", 1000, "election timeout in milliseconds")
var BalanceRegenerate *int = flag.Int("to", 500, "Token regeneration in milliseconds")
var ReplyReceiveTimeout *int = flag.Int("rrt", 10000, "Since leader may crash, lost connection, we need this timeout to count elapse")
var BatchSize *int = flag.Int("mb", 10, "max batch size")
var AutoBalance *int = flag.Int("ab", 1, "auto balance")
var Balanced *int = flag.Int("b", 0, "balanced")
var PrintIt *int = flag.Int("pp", 0, "auto balance")
var Recovered *int = flag.Int("rec", 0, "auto balance")

// Client
var ReqsNum *int = flag.Int("q", 1000, "Total number of requests. Defaults to 5000.")
var Writes *int = flag.Int("w", 70, "Percentage of updates (Writes). Defaults to 100%.")
var Rounds *int = flag.Int("rnd", 10, "Split the total number of requests into this many Rounds, and do Rounds sequentially. Defaults to 1.")
var Conflicts *int = flag.Int("c", 0, "Percentage of conflicts. Defaults to 0%")

const CHAN_BUFFER_SIZE = 60000
const LOG_SIZE = 512 * 1024
const PAXOS_LOG_SIZE = 1024 * 1024 * 20
const RecoveryInterval = 1 // 1 second
const TRUE = uint8(1)
const FALSE = uint8(0)

var TickTime *int = flag.Int("tt", 5, "Tick time in milliseconds")
var MAX_BATCH = 2
var Auto_Balance = true
var PrintPerSec = false //Modify this is not the correct way, it related to PP
var StoreLog = false

const Read_Local = true //For raft/shipyard, we do not need replicate reads
const Fail_Prone = true // Need reconnection
const PrintApportion = false

const LogFile = true
const Fake_recovery = true
