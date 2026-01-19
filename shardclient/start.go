package shardclient

func StartClient() {
	cm := NewClientManager()
	cm.writePercent = 20
	cm.conflicts = 100
	cm.round = 1000
	cm.reqsPerRound = 1000
	cm.Init()
	cm.FindLeaders()
	cm.StartSingleTest()
	//cm.StartConcurrentTest()
	cm.Close()
}
