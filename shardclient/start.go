package shardclient

func StartClient1() {
	cm := NewClientManager()
	cm.writePercent = 20
	cm.conflicts = 100
	cm.round = 10000
	cm.reqsPerRound = 1000
	cm.Init()
	cm.FindLeaders()
	cm.StartSingleTest()
	//cm.StartConcurrentTest()
	cm.Close()
}

func StartClient2() {
	cm := NewClientManager()
	cm.writePercent = 20
	cm.conflicts = 100
	cm.round = 10000
	cm.reqsPerRound = 1000
	cm.Init()
	cm.FindLeaders()
	//cm.StartSingleTest()
	cm.StartConcurrentTest()
	cm.Close()
}
