package config

type ApportionType int

const (
	TRandom ApportionType = iota
	TFrequency
	TUsage
	TMastered
)

var CurrentApportionType = TMastered
