package redis

import (
	"math/rand"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

var benchmarkHits = []test.Case{
	{
		Qname: "example.com.", Qtype: dns.TypeSOA,
		Answer: []dns.RR{
			test.SOA("example.com. 300 IN SOA ns1.example.com. hostmaster.example.com. 0 44 55 66 100"),
		},
	},
	{
		Qname: "x.example.com.", Qtype: dns.TypeA,
		Answer: []dns.RR{
			test.A("x.example.com. 300 IN A 1.2.3.4"),
			test.A("x.example.com. 300 IN A 5.6.7.8"),
		},
	},
	{
		Qname: "host3.example.net.", Qtype: dns.TypeTXT,
		Answer: []dns.RR{
			test.TXT("host3.example.net. 300 IN TXT \"this is a wildcard\""),
		},
	},
}

var benchmarkMisses = []test.Case{
	{Qname: "q.example.com.", Qtype: dns.TypeA, Rcode: dns.RcodeNameError},
	{Qname: "w.example.com.", Qtype: dns.TypeA, Rcode: dns.RcodeNameError},
	{Qname: "e.example.com.", Qtype: dns.TypeA, Rcode: dns.RcodeNameError},
}

func BenchmarkHit(b *testing.B) {
	plugin := newBenchmarkPlugin(b)
	queries := benchmarkMessages(benchmarkHits)
	source := rand.New(rand.NewSource(1))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := dnstest.NewRecorder(&test.ResponseWriter{})
		plugin.ServeDNS(context.TODO(), rec, queries[source.Intn(len(queries))])
	}
}

func BenchmarkMiss(b *testing.B) {
	plugin := newBenchmarkPlugin(b)
	queries := benchmarkMessages(benchmarkMisses)
	source := rand.New(rand.NewSource(1))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := dnstest.NewRecorder(&test.ResponseWriter{})
		plugin.ServeDNS(context.TODO(), rec, queries[source.Intn(len(queries))])
	}
}

func benchmarkMessages(cases []test.Case) []*dns.Msg {
	messages := make([]*dns.Msg, 0, len(cases))
	for _, tc := range cases {
		messages = append(messages, tc.Msg())
	}
	return messages
}

func newBenchmarkPlugin(b *testing.B) *Redis {
	b.Helper()

	backend := defaultLookupBackend()
	cache, err := newDiskCache(cacheConfig{
		path:            b.TempDir(),
		maxEntries:      128,
		maxEntrySize:    defaultCacheMaxEntrySize,
		cleanupInterval: time.Minute,
	})
	if err != nil {
		b.Fatalf("newDiskCache() error = %v", err)
	}

	plugin := &Redis{
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
		b.Fatalf("Initialize() error = %v", err)
	}
	return plugin
}
