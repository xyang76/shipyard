package shard

import (
	"Mix/config"
	"Mix/state"
)

type ShardInfo struct {
	Shards   []int32
	ShardNum int
}

// Constructor
func NewShardInfo() *ShardInfo {
	shardNum := *config.ShardNum
	if config.CurrentApproach == config.Raft {
		shardNum = 1
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
