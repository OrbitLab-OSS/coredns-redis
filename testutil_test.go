package redis

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin/pkg/fall"
)

type fakeBackend struct {
	mu sync.Mutex

	zones    []string
	zoneData map[string]map[string]struct{}
	records  map[string]map[string]string

	listZonesErr error
	loadZoneErr  error
	getRecordErr error

	listZonesCalls int
	loadZoneCalls  int
	getRecordCalls int
	commands       []string
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		zoneData: map[string]map[string]struct{}{},
		records:  map[string]map[string]string{},
	}
}

func (b *fakeBackend) ListZones() ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.listZonesCalls++
	b.commands = append(b.commands, "SCAN")
	if b.listZonesErr != nil {
		return nil, b.listZonesErr
	}
	return append([]string(nil), b.zones...), nil
}

func (b *fakeBackend) LoadZone(zone string) (*Zone, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.loadZoneCalls++
	b.commands = append(b.commands, "HKEYS")
	if b.loadZoneErr != nil {
		return nil, b.loadZoneErr
	}

	locations := make(map[string]struct{}, len(b.zoneData[zone]))
	for location := range b.zoneData[zone] {
		locations[location] = struct{}{}
	}

	return &Zone{
		Name:        normalizeZone(zone),
		Locations:   locations,
		RefreshedAt: time.Now(),
	}, nil
}

func (b *fakeBackend) GetRecord(zone, label string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.getRecordCalls++
	b.commands = append(b.commands, "HGET")
	if b.getRecordErr != nil {
		return "", b.getRecordErr
	}

	value, ok := b.records[normalizeZone(zone)][label]
	if !ok {
		return "", errRecordNotFound
	}
	if len(value) > defaultRecordSizeLimit {
		return "", errRecordTooLarge
	}
	return value, nil
}

func (b *fakeBackend) setZone(zone string, records map[string]string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	zone = normalizeZone(zone)
	b.zones = appendIfMissing(b.zones, zone)
	b.records[zone] = make(map[string]string, len(records))
	b.zoneData[zone] = make(map[string]struct{}, len(records))
	for label, value := range records {
		b.records[zone][label] = value
		b.zoneData[zone][label] = struct{}{}
	}
}

func (b *fakeBackend) setRecord(zone, label, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	zone = normalizeZone(zone)
	b.zones = appendIfMissing(b.zones, zone)
	if b.records[zone] == nil {
		b.records[zone] = map[string]string{}
	}
	if b.zoneData[zone] == nil {
		b.zoneData[zone] = map[string]struct{}{}
	}
	b.records[zone][label] = value
	b.zoneData[zone][label] = struct{}{}
}

func (b *fakeBackend) removeRecord(zone, label string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	zone = normalizeZone(zone)
	delete(b.records[zone], label)
	delete(b.zoneData[zone], label)
}

func (b *fakeBackend) setZoneLocations(zone string, locations ...string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	zone = normalizeZone(zone)
	b.zones = appendIfMissing(b.zones, zone)
	b.zoneData[zone] = map[string]struct{}{}
	for _, location := range locations {
		b.zoneData[zone][location] = struct{}{}
	}
}

func (b *fakeBackend) callCounts() (int, int, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.listZonesCalls, b.loadZoneCalls, b.getRecordCalls
}

func (b *fakeBackend) seenCommand(command string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, current := range b.commands {
		if current == command {
			return true
		}
	}
	return false
}

func appendIfMissing(values []string, value string) []string {
	for _, current := range values {
		if current == value {
			return values
		}
	}
	return append(values, value)
}

func newTestPlugin(t *testing.T, backend *fakeBackend) *Redis {
	t.Helper()

	cache, err := newDiskCache(cacheConfig{
		path:            t.TempDir(),
		maxEntries:      128,
		maxEntrySize:    defaultCacheMaxEntrySize,
		cleanupInterval: time.Minute,
	})
	if err != nil {
		t.Fatalf("newDiskCache() error = %v", err)
	}

	plugin := &Redis{
		Fall:          fall.F{},
		backend:       backend,
		cache:         cache,
		zones:         newZoneStore(backend, time.Hour),
		defaultTTL:    defaultPluginTTL,
		maxRecordSize: defaultRecordSizeLimit,
		metrics:       &pluginMetrics{},
		logLimiter: &rateLimitedLogger{
			interval: time.Millisecond,
			last:     map[string]time.Time{},
		},
	}

	if err := plugin.zones.Initialize(); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	return plugin
}

func defaultLookupBackend() *fakeBackend {
	backend := newFakeBackend()
	backend.setZone("example.com.", map[string]string{
		"@":         `{"soa":{"ttl":300,"minttl":100,"mbox":"hostmaster.example.com.","ns":"ns1.example.com.","refresh":44,"retry":55,"expire":66}}`,
		"x":         `{"a":[{"ttl":300,"ip":"1.2.3.4"},{"ttl":300,"ip":"5.6.7.8"}],"aaaa":[{"ttl":300,"ip":"::1"}],"txt":[{"ttl":300,"text":"foo"},{"ttl":300,"text":"bar"}],"ns":[{"ttl":300,"host":"ns1.example.com."},{"ttl":300,"host":"ns2.example.com."}],"mx":[{"ttl":300,"host":"mx1.example.com.","preference":10},{"ttl":300,"host":"mx2.example.com.","preference":10}]}`,
		"y":         `{"cname":[{"ttl":300,"host":"x.example.com."}]}`,
		"ns1":       `{"a":[{"ttl":300,"ip":"2.2.2.2"}]}`,
		"ns2":       `{"a":[{"ttl":300,"ip":"3.3.3.3"}]}`,
		"_sip._tcp": `{"srv":[{"ttl":300,"target":"sip.example.com.","port":555,"priority":10,"weight":100}]}`,
		"sip":       `{"a":[{"ttl":300,"ip":"7.7.7.7"}],"aaaa":[{"ttl":300,"ip":"::1"}]}`,
		"mail":      `{"a":[{"ttl":300,"ip":"9.9.9.9"}]}`,
		"rel":       `{"mx":[{"ttl":120,"host":"mail","preference":10}]}`,
		"caa":       `{"caa":[{"ttl":180,"flag":0,"tag":"issue","value":"letsencrypt.org"}]}`,
	})
	backend.setZone("example.net.", map[string]string{
		"@":               `{"soa":{"ttl":300,"minttl":100,"mbox":"hostmaster.example.net.","ns":"ns1.example.net.","refresh":44,"retry":55,"expire":66},"ns":[{"ttl":300,"host":"ns1.example.net."},{"ttl":300,"host":"ns2.example.net."}]}`,
		"sub.*":           `{"txt":[{"ttl":300,"text":"literal star"}]}`,
		"host1":           `{"a":[{"ttl":300,"ip":"5.5.5.5"}]}`,
		"subdel":          `{"ns":[{"ttl":300,"host":"ns1.subdel.example.net."},{"ttl":300,"host":"ns2.subdel.example.net."}]}`,
		"*":               `{"txt":[{"ttl":300,"text":"this is a wildcard"}],"mx":[{"ttl":300,"host":"host1.example.net.","preference":10}]}`,
		"_ssh._tcp.host1": `{"srv":[{"ttl":300,"target":"tcp.example.com.","port":123,"priority":10,"weight":100}]}`,
		"_ssh._tcp.host2": `{"srv":[{"ttl":300,"target":"tcp.example.com.","port":123,"priority":10,"weight":100}]}`,
	})
	backend.setZone("example.test.", map[string]string{
		"@":     `{"soa":{"ttl":300,"minttl":100,"mbox":"hostmaster.example.test.","ns":"ns1.example.test.","refresh":44,"retry":55,"expire":66}}`,
		"host1": `{"a":[{"ttl":300,"ip":"5.5.5.5"}`,
	})
	return backend
}

func mustNoErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertErrIs(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}
