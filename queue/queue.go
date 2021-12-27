package queue

import (
	"context"
	"time"
	"github.com/asuffield/shedding"
	"github.com/benbjohnson/clock"
	"sort"
)

type element[T any] struct {
	ctx context.Context
	cancel context.CancelFunc
	crit shedding.Criticality
	v T
	enqueued time.Time
}

type Config struct {
	Clock clock.Clock
	TimingHistory int // Estimate the dequeue rate using this many recent data points
	DiscardOutliers int // Remove this many values from the highest and lowest end of the range. Not recommended for value values of TimingHistory, because this requires a sort.
}

func (c *Config) defaults() {
	if c.Clock == nil {
		c.Clock = clock.New()
	}
	if c.TimingHistory == 0 {
		c.TimingHistory = 100
		c.DiscardOutliers = 1
	}
}

type Queue[T any] struct {
	Config Config

	l []element[T]
	lastDequeue time.Time
	recentDequeue []time.Duration
	expectedWait time.Duration // expected wait time per item in the queue
	expectedWaitAt time.Time // time when expectedWait was last computed
}

type Criticality int

func (q *Queue[T]) Insert(ctx context.Context, crit shedding.Criticality, v T, cancel context.CancelFunc) {
	q.Config.defaults()

	q.l = append(q.l, element[T]{ctx, cancel, crit, v, q.Config.Clock.Now()})
	go func() {
		// Shed immediately in case this new element should be discarded
		q.shed()

		// Wait for the context to be cancelled
		<-ctx.Done()
		// Shed to remove cancelled items from the queue
		q.shed()
	}()
}

func (q *Queue[T]) Remove() T {
	q.Config.defaults()
	q.shed()

	var result T
	if len(q.l) == 0 {
		return result
	}
	result = q.l[0].v
	copy(q.l, q.l[1:])
	q.l[len(q.l)-1] = element[T]{}
	q.l = q.l[:len(q.l)-1]

	now := q.Config.Clock.Now()
	interval := now.Sub(q.lastDequeue)
	q.lastDequeue = now
	if len(q.recentDequeue) >= q.Config.TimingHistory {
		// + 1 because we want to make room to add one
		start := 1 + len(q.recentDequeue) - q.Config.TimingHistory
		copy(q.recentDequeue, q.recentDequeue[start:])
		q.recentDequeue = q.recentDequeue[:q.Config.TimingHistory-1]
	}
	q.recentDequeue = append(q.recentDequeue, interval)
	return result
}

func (q *Queue[T]) Len() int {
	q.Config.defaults()

	return len(q.l)
}

func (q *Queue[T]) updateTiming() {
	if len(q.recentDequeue) < q.Config.TimingHistory {
		// Not enough data to estimate yet - this prevents shedding based on deadlines
		q.expectedWait = 0
		return
	}
	if !q.expectedWaitAt.Before(q.lastDequeue) {
		// No new data, no need to recompute
		return
	}

	intervals := q.recentDequeue
	if q.Config.DiscardOutliers > 0 {
		intervals := make([]time.Duration, len(q.recentDequeue))
		copy(intervals, q.recentDequeue)
		sort.Slice(intervals, func(i, j int) bool {return intervals[i] < intervals[j]})
		intervals = intervals[q.Config.DiscardOutliers:len(intervals)-q.Config.DiscardOutliers]
	}

	var total time.Duration
	for _, t := range intervals {
		total += t
	}
	q.expectedWait = total / time.Duration(len(intervals))
	q.expectedWaitAt = q.lastDequeue
}

func (q *Queue[T]) shed() {
	q.mux.Lock()
	defer q.mux.Unlock()
	q.Config.defaults()
	q.updateTiming()

	// First, shed any entries which have already missed their deadline

	l := []element[T]{}
	for _, e := range q.l {
		if e.ctx.Err() != nil {
			// Shed anything that's dead already
			e.cancel()
			continue
		}
		l = append(l, e)
	}

	// Next we want to shed anything that isn't expected to meet its deadline -
	// but this is a bit trickier, because if a higher-criticality entry would
	// be doable then we need to shed before that point. We use a multiple-list
	// approach to make backtracking easy.
	byCrit := map[shedding.Criticality][]int{} // values are indexes into l
	for i, e := range l {
		byCrit[e.crit] = append(byCrit[e.crit], i)
	}

	// Baseline time for checking ctx.Deadline
	now := q.Config.Clock.Now()
	

		if at, ok := e.ctx.Deadline(); ok && at.Before(expectedDequeue) {
			// This item is not expected to be done before its deadline; shed it immediately
			e.cancel()
			continue
		}

		expectedDequeue = expectedDequeue.Add(q.expectedWait)
		l = append(l, e)
	}

	q.l = l
}