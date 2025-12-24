package checker

import "math/rand"

const (
	SEED = 1
)

type Sample struct {
	sampleCnt int64
	totalCnt  int64
	source    *rand.Rand
}

func NewSample(sampleCnt, totalCnt int64) *Sample {
	return &Sample{
		sampleCnt: sampleCnt,
		totalCnt:  totalCnt,
		source:    rand.New(rand.NewSource(SEED)),
	}
}

func (s *Sample) Hit() bool {
	// sampleCnt == 0 means disable sampling, compare all data
	if s.sampleCnt == 0 || s.sampleCnt >= s.totalCnt {
		return true
	}

	return s.source.Int63n(s.totalCnt) < s.sampleCnt
}
