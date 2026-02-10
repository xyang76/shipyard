package config

import (
	"math/rand"
	"time"
)

type ApportionType int

const (
	TRandom ApportionType = iota
	TFrequency
	TUsage
	TMastered
)

var CurrentApportionType = TMastered

type Approach int

const (
	Shipyard Approach = iota
	MultiRaft
	Raft
	EPaxos
	GPaxos
	Mencius
	Paxos
	Base
)

type Instance int

const (
	Test Instance = iota
	Master
	Server
	Client
	ClientPerSec
	ClientWithFail
)

var CurrentInstance Instance
var CurrentApproach Approach

func SetEnvironment() {
	CurrentInstance = Instance(*role)
	CurrentApproach = Approach(*app)
	MAX_BATCH = *BatchSize
	Auto_Balance = *AutoBalance != 0
	PrintPerSec = *PrintIt != 0
}

// electionTimeout generates a pseudo-random election timeout duration.
func RandomElectionTimeout() time.Duration {
	min := *HeartBeatTimeout
	max := *HeartBeatTimeout * 2
	return time.Duration(min+rand.Intn(max-min)) * time.Millisecond
}
