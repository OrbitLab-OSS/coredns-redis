package redis

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/fall"
	clog "github.com/coredns/coredns/plugin/pkg/log"
)

var log = clog.NewWithPlugin("redis")

const (
	defaultRedisAddress      = "localhost:6379"
	defaultPluginTTL         = 300
	defaultZoneRefresh       = time.Minute
	defaultScanCount         = 1000
	defaultCacheMaxEntries   = 10000
	defaultCacheCleanup      = 10 * time.Minute
	defaultCacheMaxEntrySize = 1 << 20
	defaultRedisMaxIdle      = 8
	defaultRedisMaxActive    = 64
	defaultRedisIdleTimeout  = 5 * time.Minute
	defaultBorrowCheck       = 30 * time.Second
	defaultRecordSizeLimit   = 1 << 20
	defaultDatabase          = 0
	defaultTtl               = 360
	hostmaster               = "hostmaster"
	transferLength           = 1000
)

type Redis struct {
	Next plugin.Handler
	Fall fall.F

	backend       recordBackend
	cache         *diskCache
	zones         *zoneStore
	defaultTTL    uint32
	maxRecordSize int

	metrics    *pluginMetrics
	logLimiter *rateLimitedLogger
}

type rateLimitedLogger struct {
	interval time.Duration

	mu   sync.Mutex
	last map[string]time.Time
}

type pluginMetrics struct {
	cacheHits  atomic.Uint64
	cacheMiss  atomic.Uint64
	redisReads atomic.Uint64
}

func newRedisPlugin(cfg pluginConfig) (*Redis, error) {
	backend := newRedisBackend(cfg.backend)

	cache, err := newDiskCache(cfg.cache)
	if err != nil {
		return nil, err
	}

	plugin := &Redis{
		Fall:          cfg.fall,
		backend:       backend,
		cache:         cache,
		zones:         newZoneStore(backend, cfg.zoneRefresh),
		defaultTTL:    cfg.defaultTTL,
		maxRecordSize: cfg.maxRecordSize,
		metrics:       &pluginMetrics{},
		logLimiter: &rateLimitedLogger{
			interval: 30 * time.Second,
			last:     make(map[string]time.Time),
		},
	}

	if err := plugin.zones.Initialize(); err != nil {
		return nil, err
	}
	return plugin, nil
}

func (r *Redis) Name() string { return "redis" }

func (r *Redis) serial() uint32 {
	return uint32(time.Now().Unix())
}

func (r *Redis) answersForType(qtype, qname string, z *Zone, record *Record) (answers, extras []dns.RR, ok bool) {
	switch qtype {
	case "A":
		answers, extras = r.A(qname, z, record)
	case "AAAA":
		answers, extras = r.AAAA(qname, z, record)
	case "CNAME":
		answers, extras = r.CNAME(qname, z, record)
	case "TXT":
		answers, extras = r.TXT(qname, z, record)
	case "NS":
		answers, extras = r.NS(qname, z, record)
	case "MX":
		answers, extras = r.MX(qname, z, record)
	case "SRV":
		answers, extras = r.SRV(qname, z, record)
	case "SOA":
		answers, extras = r.SOA(qname, z, record)
	case "CAA":
		answers, extras = r.CAA(qname, z, record)
	default:
		return nil, nil, false
	}
	return answers, extras, true
}

func (r *Redis) findLocation(qname string, z *Zone) string {
	label, ok := relativeNameForZone(qname, z.Name)
	if !ok {
		return ""
	}
	if label == "@" {
		return "@"
	}
	if _, exists := z.Locations[label]; exists {
		return label
	}

	labels := strings.Split(label, ".")
	closest := "@"
	for i := 1; i < len(labels); i++ {
		ancestor := strings.Join(labels[i:], ".")
		if nameExists(ancestor, z) {
			closest = ancestor
			break
		}
	}

	wildcard := "*"
	if closest != "@" {
		wildcard = "*." + closest
	}
	if _, exists := z.Locations[wildcard]; exists {
		return wildcard
	}
	return ""
}

func nameExists(label string, z *Zone) bool {
	if label == "" || label == "@" {
		return true
	}
	if _, exists := z.Locations[label]; exists {
		return true
	}

	suffix := "." + label
	for candidate := range z.Locations {
		if strings.HasSuffix(candidate, suffix) {
			return true
		}
	}
	return false
}

func decodeScanReply(reply interface{}) (*RedisScanReply, error) {
	values, ok := reply.([]interface{})
	if !ok || len(values) != 2 {
		return nil, errors.New("unexpected SCAN reply shape")
	}

	cursorBytes, ok := values[0].([]byte)
	if !ok {
		return nil, errors.New("unexpected SCAN cursor type")
	}
	cursor, err := strconv.Atoi(string(cursorBytes))
	if err != nil {
		return nil, err
	}

	keyValues, ok := values[1].([]interface{})
	if !ok {
		return nil, errors.New("unexpected SCAN keys type")
	}

	keys := make([]string, 0, len(keyValues))
	for _, value := range keyValues {
		keyBytes, ok := value.([]byte)
		if !ok {
			return nil, errors.New("unexpected SCAN key value type")
		}
		keys = append(keys, string(keyBytes))
	}

	return &RedisScanReply{cursor: cursor, keys: keys}, nil
}

type RedisScanReply struct {
	cursor int
	keys   []string
}

func split255(s string) []string {
	if len(s) <= 255 {
		return []string{s}
	}

	parts := make([]string, 0, (len(s)/255)+1)
	for start := 0; start < len(s); start += 255 {
		end := start + 255
		if end > len(s) {
			end = len(s)
		}
		parts = append(parts, s[start:end])
	}
	return parts
}

func (l *rateLimitedLogger) shouldLog(key string) bool {
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()

	last, ok := l.last[key]
	if ok && now.Sub(last) < l.interval {
		return false
	}
	l.last[key] = now
	return true
}

func (r *Redis) logRedisError(key, format string, args ...interface{}) {
	if r.logLimiter.shouldLog("redis:" + key) {
		log.Errorf(format, args...)
	}
}

func (r *Redis) logCacheError(key, format string, args ...interface{}) {
	if r.logLimiter.shouldLog("cache:" + key) {
		log.Errorf(format, args...)
	}
}

func (m *pluginMetrics) addCacheHit() {
	m.cacheHits.Add(1)
}

func (m *pluginMetrics) addCacheMiss() {
	m.cacheMiss.Add(1)
}

func (m *pluginMetrics) addRedisRead() {
	m.redisReads.Add(1)
}

type pluginConfig struct {
	fall          fall.F
	defaultTTL    uint32
	zoneRefresh   time.Duration
	maxRecordSize int
	backend       backendConfig
	cache         cacheConfig
}
