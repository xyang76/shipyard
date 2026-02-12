package clientsep

import (
	"Mix/config"
)

func StartClient1() {
	cm := NewClientManager()
	cm.writePercent = 20
	cm.conflicts = 100
	cm.round = 10000
	cm.reqsPerRound = *config.ReqsNum
	cm.Init()
	cm.FindLeaders()
	cm.StartSingleTest()
	//cm.StartConcurrentTest()
	cm.Close()
}

func StartClient2() {
	cm := NewClientManager()
	cm.writePercent = *config.Writes
	cm.conflicts = *config.Conflicts
	cm.round = *config.Rounds
	cm.reqsPerRound = *config.ReqsNum
	cm.Init()
	cm.FindLeaders()
	//cm.StartSingleTest()
	cm.StartConcurrentTest()
	cm.Close()
}
