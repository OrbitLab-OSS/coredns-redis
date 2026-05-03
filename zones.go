package redis

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/coredns/coredns/plugin"
)

type zoneStore struct {
	backend         recordBackend
	refreshInterval time.Duration

	mu          sync.RWMutex
	zones       []string
	lastRefresh time.Time
	zoneStates  map[string]*zoneState

	refreshMu        sync.Mutex
	asyncRefreshBusy atomic.Bool
}

type zoneState struct {
	refreshMu sync.Mutex

	mu   sync.RWMutex
	zone *Zone
}

func newZoneStore(backend recordBackend, refreshInterval time.Duration) *zoneStore {
	return &zoneStore{
		backend:         backend,
		refreshInterval: refreshInterval,
		zoneStates:      make(map[string]*zoneState),
	}
}

func (s *zoneStore) Initialize() error {
	return s.refreshZones()
}

func (s *zoneStore) Match(qname string) string {
	zones, lastRefresh := s.snapshotZones()
	match := plugin.Zones(zones).Matches(qname)
	if match != "" {
		if s.isStale(lastRefresh) {
			s.scheduleRefresh()
		}
		return match
	}

	if !s.isStale(lastRefresh) {
		return ""
	}

	if err := s.refreshZones(); err != nil {
		return ""
	}

	zones, _ = s.snapshotZones()
	return plugin.Zones(zones).Matches(qname)
}

func (s *zoneStore) Get(zone string) (*Zone, error) {
	state := s.getZoneState(zone)
	if current := state.snapshot(); current != nil && !s.isStale(current.RefreshedAt) {
		return current, nil
	}

	state.refreshMu.Lock()
	defer state.refreshMu.Unlock()

	if current := state.snapshot(); current != nil && !s.isStale(current.RefreshedAt) {
		return current, nil
	}

	loaded, err := s.backend.LoadZone(zone)
	if err != nil {
		if current := state.snapshot(); current != nil {
			return current, nil
		}
		return nil, err
	}

	state.store(loaded)
	return loaded, nil
}

func (s *zoneStore) RefreshZone(zone string) (*Zone, error) {
	state := s.getZoneState(zone)
	state.refreshMu.Lock()
	defer state.refreshMu.Unlock()

	loaded, err := s.backend.LoadZone(zone)
	if err != nil {
		if current := state.snapshot(); current != nil {
			return current, nil
		}
		return nil, err
	}

	state.store(loaded)
	return loaded, nil
}

func (s *zoneStore) refreshZones() error {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	if !s.isStale(s.lastRefresh) && len(s.zones) > 0 {
		return nil
	}

	zones, err := s.backend.ListZones()
	if err != nil {
		return err
	}

	states := make(map[string]*zoneState, len(zones))

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, zone := range zones {
		if existing, ok := s.zoneStates[zone]; ok {
			states[zone] = existing
			continue
		}
		states[zone] = &zoneState{}
	}

	s.zoneStates = states
	s.zones = zones
	s.lastRefresh = time.Now()
	return nil
}

func (s *zoneStore) scheduleRefresh() {
	if !s.asyncRefreshBusy.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer s.asyncRefreshBusy.Store(false)
		_ = s.refreshZones()
	}()
}

func (s *zoneStore) snapshotZones() ([]string, time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zones := append([]string(nil), s.zones...)
	return zones, s.lastRefresh
}

func (s *zoneStore) getZoneState(zone string) *zoneState {
	s.mu.Lock()
	defer s.mu.Unlock()

	if state, ok := s.zoneStates[zone]; ok {
		return state
	}

	state := &zoneState{}
	s.zoneStates[zone] = state
	return state
}

func (s *zoneStore) isStale(ts time.Time) bool {
	if ts.IsZero() {
		return true
	}
	return time.Since(ts) >= s.refreshInterval
}

func (s *zoneState) snapshot() *Zone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.zone
}

func (s *zoneState) store(zone *Zone) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.zone = zone
}
