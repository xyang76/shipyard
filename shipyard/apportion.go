package shipyard

import (
	"Mix/config"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
)

type Apportion struct {
	replica     *Replica
	typ         config.ApportionType
	exeCommands int
	threshold   int
	timeCount   int
	cpuUsage    int
	lastUsage   int
	lastCommand int
}

func NewApportion(replica *Replica) *Apportion {
	app := &Apportion{
		typ:         config.TMastered,
		replica:     replica,
		threshold:   0,
		timeCount:   1,
		cpuUsage:    0,
		lastUsage:   0,
		lastCommand: 0,
	}
	if config.PrintApportion {
		app.StartTimer()
	}
	switch app.typ {
	case config.TFrequency:
		app.threshold = 1000
	case config.TUsage:
		app.threshold = 20
	}
	return app
}

func (a *Apportion) StartTimer() {
	ticker := time.NewTicker(1 * time.Second)
	stop := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				// Execute the function
				a.executeCount()
			case <-stop:
				// Stop the ticker
				ticker.Stop()
				return
			}
		}
	}()
}

func encodeApportion(val int) int {
	//val := a.value()
	invert := ^val & 0xFF
	return invert
	//return (invert << 16) | rand.Intn(len(a.replica.peerIds))*10
}

func decodeApportion(apportion int) int {
	//val := ^(apportion >> 16) & 0xFF
	//return val
	return ^apportion & 0xFF
}

func (a *Apportion) value() int {
	switch a.typ {
	case config.TFrequency:
		return a.exeCommands / a.timeCount
	case config.TUsage:
		return a.cpuUsage * 100 / a.timeCount
	case config.TMastered:
		return a.replica.getCurrentLeaderSize()
	case config.TRandom:
		return rand.Intn(len(a.replica.peerIds) * 10)
	}
	return 0
}

func (a *Apportion) IncExecuteCommand() {
	a.exeCommands++
}

func (a *Apportion) Imbalance(apportion int) bool {
	actualNum := decodeApportion(apportion)
	switch a.typ {
	case config.TFrequency:
		return actualNum-a.threshold-a.value() > 0
	case config.TUsage:
		return actualNum-a.threshold-a.value() > 0
	case config.TMastered:
		return actualNum-a.value() > 1
	case config.TRandom:
		return false
	}
	return false
}

func (a *Apportion) executeCount() {
	a.timeCount++
	if a.typ == config.TUsage {
		percentages, err := cpu.Percent(1*time.Second, false)
		if err != nil {
			fmt.Println("Error retrieving CPU usage:", err)
			return
		}
		a.cpuUsage += int(percentages[0])
	}

	if a.timeCount%5 == 0 {
		//dlog.Info("Replica%v of %v: CPU usage:%v, execute commands:%v, leading:%v", a.replica.Id, a.timeCount,
		//	(a.cpuUsage-a.lastUsage)/5, a.exeCommands-a.lastCommand, a.replica.leadingShards)
		now := time.Now()
		hours := now.Hour()
		seconds := now.Second()
		minutes := now.Minute()
		log.Printf("Replica%v of %v(h:%v, %v): leading:%v", a.replica.Id, a.timeCount,
			hours, minutes*60+seconds, a.replica.leadingShards)
		a.lastUsage = a.cpuUsage
		a.lastCommand = a.exeCommands
	}
	// Reset time count
	if a.timeCount%100 == 0 {
		a.cpuUsage = a.cpuUsage / a.timeCount
		a.exeCommands = a.exeCommands / a.timeCount
		a.lastUsage = a.cpuUsage
		a.lastCommand = a.exeCommands
		a.timeCount = 1
	}
}
