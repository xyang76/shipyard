package config

type Shard struct {
	ID string `json:"id"`
}

type ShardConfig struct {
	Shards []Shard `json:"shards"`
}
