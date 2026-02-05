package shard

import (
	"Mix/config"
	"Mix/state"
	"math/rand"
)

type ShardInfo struct {
	Shards   []int32
	ShardNum int
}

// Constructor
func NewShardInfo() *ShardInfo {
	shardNum := 1
	if config.CurrentApproach == config.MultiRaft ||
		config.CurrentApproach == config.Shipyard {
		shardNum = *config.ShardNum
	}

	shards := make([]int32, shardNum)
	for i := 0; i < shardNum; i++ {
		shards[i] = int32(i)
	}

	return &ShardInfo{
		Shards:   shards,
		ShardNum: shardNum,
	}
}

func (s *ShardInfo) GetShardNum() int {
	return s.GetShardNum()
}

func (s *ShardInfo) GetShards() []int32 {
	return s.Shards
}

func (s *ShardInfo) isShardId() bool {
	return false
}

func (s *ShardInfo) GetShardId(k state.Key) (int32, error) {
	return int32(uint64(k) % uint64(s.ShardNum)), nil
}

func (s *ShardInfo) GetKey(shardID int32, reqID int32) state.Key {
	return state.Key(
		uint64(reqID)*uint64(s.ShardNum) + uint64(shardID),
	)
}

func (s *ShardInfo) GetUnbalancedKey(reqID int32) state.Key {
	sid := shardSpecify(randomCharWeighted())
	return state.Key(
		uint64(reqID)*uint64(s.ShardNum) + uint64(sid),
	)
}

func shardSpecify(letter string) int {
	switch letter {
	case "E", "T", "A":
		return 0
	case "O", "I", "N":
		return 1
	case "S", "H", "R":
		return 2
	case "L", "D", "C":
		return 3
	case "U", "M", "W":
		return 4
	case "F", "G", "Y":
		return 5
	case "P", "B", "V":
		return 6
	case "K", "X", "J":
		return 7
	case "Q", "Z":
		return 8
	default:
		return 8
	}
	return 0
}

func randomCharWeighted() string {
	// Approximate English letter frequencies
	weights := []struct {
		char   rune
		weight int
	}{
		{'A', 8167}, {'B', 1492}, {'C', 2782}, {'D', 4253}, {'E', 12702},
		{'F', 2228}, {'G', 2015}, {'H', 6094}, {'I', 6966}, {'J', 153},
		{'K', 772}, {'L', 4025}, {'M', 2406}, {'N', 6749}, {'O', 7507},
		{'P', 1929}, {'Q', 95}, {'R', 5987}, {'S', 6327}, {'T', 9056},
		{'U', 2758}, {'V', 978}, {'W', 2360}, {'X', 150}, {'Y', 1974}, {'Z', 74},
	}

	// Calculate the cumulative weights
	totalWeight := 0
	for _, w := range weights {
		totalWeight += w.weight
	}

	// Generate a random number up to the total weight
	randVal := rand.Intn(totalWeight)

	// Find the corresponding character based on cumulative weights
	for _, w := range weights {
		randVal -= w.weight
		if randVal < 0 {
			return string(w.char)
		}
	}
	return "A" // Fallback (should never reach here)
}
