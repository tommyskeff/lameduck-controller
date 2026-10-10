package backoff

import "time"

type Exponential struct {
	minimum time.Duration
	maximum time.Duration
	random  func(n int64) int64
	ceiling time.Duration
}

func NewExponential(minimum, maximum time.Duration, random func(n int64) int64) *Exponential {
	return &Exponential{minimum: minimum, maximum: maximum, random: random, ceiling: minimum}
}

func (e *Exponential) Next() time.Duration {
	delay := time.Duration(e.random(int64(e.ceiling) + 1))
	e.ceiling = min(e.ceiling*2, e.maximum)
	return delay
}

func (e *Exponential) Reset() {
	e.ceiling = e.minimum
}
