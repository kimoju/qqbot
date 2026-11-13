package dispatch

import (
	"strings"
	"sync"
	"time"
)

const (
	defaultMaxSeenEntries = 16384
	sweepOperationStep    = 256
	sweepMinInterval      = time.Minute
	compactThreshold      = 512
)

type SeenStore interface {
	Seen(kind string, id string, now time.Time, ttl time.Duration) bool
}

type queueItem struct {
	key      string
	deadline time.Time
}

type MemorySeenStore struct {
	mu         sync.Mutex
	maxEntries int
	seen       map[string]time.Time
	order      []queueItem
	head       int
	ops        int
	peakSize   int
	lastSweep  time.Time
}

func NewMemorySeenStore() *MemorySeenStore {
	return NewMemorySeenStoreWithLimit(defaultMaxSeenEntries)
}

func NewMemorySeenStoreWithLimit(maxEntries int) *MemorySeenStore {
	if maxEntries <= 0 {
		maxEntries = defaultMaxSeenEntries
	}
	return &MemorySeenStore{
		maxEntries: maxEntries,
		seen:       make(map[string]time.Time),
	}
}

func (s *MemorySeenStore) Seen(kind string, id string, now time.Time, ttl time.Duration) bool {
	id = strings.TrimSpace(id)
	if id == "" || ttl <= 0 {
		return false
	}
	key := strings.TrimSpace(kind) + ":" + id

	s.mu.Lock()
	defer s.mu.Unlock()

	if deadline, ok := s.seen[key]; ok {
		if now.Before(deadline) {
			return true
		}
		delete(s.seen, key)
	}

	s.evictExpiredHeadLocked(now)

	s.ops++
	if s.lastSweep.IsZero() {
		s.lastSweep = now
	} else if s.ops >= sweepOperationStep || now.Sub(s.lastSweep) >= sweepMinInterval {
		s.sweepLocked(now)
	}

	for len(s.seen) >= s.maxEntries && s.head < len(s.order) {
		front := s.order[s.head]
		s.order[s.head] = queueItem{}
		s.head++
		if cur, ok := s.seen[front.key]; ok && cur.Equal(front.deadline) {
			delete(s.seen, front.key)
		}
	}
	if len(s.seen) >= s.maxEntries {
		s.sweepLocked(now)
		for k := range s.seen {
			if len(s.seen) < s.maxEntries {
				break
			}
			delete(s.seen, k)
		}
	}

	deadline := now.Add(ttl)
	s.seen[key] = deadline
	s.order = append(s.order, queueItem{key: key, deadline: deadline})
	if len(s.seen) > s.peakSize {
		s.peakSize = len(s.seen)
	}

	s.maybeCompactOrderLocked()
	return false
}

func (s *MemorySeenStore) evictExpiredHeadLocked(now time.Time) {
	for s.head < len(s.order) {
		front := s.order[s.head]
		if now.Before(front.deadline) {
			break
		}
		s.order[s.head] = queueItem{}
		s.head++
		if cur, ok := s.seen[front.key]; ok && !now.Before(cur) {
			delete(s.seen, front.key)
		}
	}
}

func (s *MemorySeenStore) sweepLocked(now time.Time) {
	s.ops = 0
	s.lastSweep = now

	for existing, deadline := range s.seen {
		if !now.Before(deadline) {
			delete(s.seen, existing)
		}
	}

	nextOrder := make([]queueItem, 0, len(s.seen))
	for i := s.head; i < len(s.order); i++ {
		item := s.order[i]
		if cur, ok := s.seen[item.key]; ok && cur.Equal(item.deadline) {
			nextOrder = append(nextOrder, item)
		}
	}
	s.order = nextOrder
	s.head = 0

	if s.peakSize >= compactThreshold && len(s.seen)*4 <= s.peakSize {
		nextCap := len(s.seen) * 2
		if nextCap < 64 {
			nextCap = 64
		}
		compacted := make(map[string]time.Time, nextCap)
		for k, v := range s.seen {
			compacted[k] = v
		}
		s.seen = compacted
		s.peakSize = len(s.seen)
	}
}

func (s *MemorySeenStore) maybeCompactOrderLocked() {
	if s.head < compactThreshold || s.head*2 < len(s.order) {
		return
	}
	live := len(s.order) - s.head
	next := make([]queueItem, live, max(live*2, 64))
	copy(next, s.order[s.head:])
	s.order = next
	s.head = 0
}

func (s *MemorySeenStore) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}
