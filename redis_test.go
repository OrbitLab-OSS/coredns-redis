package redis

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/miekg/dns"
)

func TestDialAddress(t *testing.T) {
	tests := []struct {
		name        string
		address     string
		wantNetwork string
		wantAddress string
	}{
		{name: "tcp", address: "localhost:6379", wantNetwork: "tcp", wantAddress: "localhost:6379"},
		{name: "unix path", address: "/var/run/redis.sock", wantNetwork: "unix", wantAddress: "/var/run/redis.sock"},
		{name: "unix prefix", address: "unix:/var/run/redis.sock", wantNetwork: "unix", wantAddress: "/var/run/redis.sock"},
		{name: "unix url", address: "unix:///var/run/redis.sock", wantNetwork: "unix", wantAddress: "/var/run/redis.sock"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			network, address := dialAddress(tc.address)
			if network != tc.wantNetwork || address != tc.wantAddress {
				t.Fatalf("dialAddress(%q) = (%q, %q), want (%q, %q)", tc.address, network, address, tc.wantNetwork, tc.wantAddress)
			}
		})
	}
}

func TestDecodeScanReply(t *testing.T) {
	reply, err := decodeScanReply([]interface{}{
		[]byte("17"),
		[]interface{}{[]byte("example.org."), []byte("example.net.")},
	})
	mustNoErr(t, err)
	if reply.cursor != 17 {
		t.Fatalf("cursor = %d, want 17", reply.cursor)
	}
	if !reflect.DeepEqual(reply.keys, []string{"example.org.", "example.net."}) {
		t.Fatalf("keys = %v, want %v", reply.keys, []string{"example.org.", "example.net."})
	}
}

func TestDecodeScanReplyRejectsMalformedData(t *testing.T) {
	_, err := decodeScanReply([]interface{}{[]byte("bad"), []interface{}{}})
	if err == nil {
		t.Fatal("expected error for malformed cursor")
	}
}

func TestSplit255(t *testing.T) {
	short := split255("hello")
	if !reflect.DeepEqual(short, []string{"hello"}) {
		t.Fatalf("split255(short) = %v", short)
	}

	long := strings.Repeat("a", 511)
	parts := split255(long)
	if len(parts) != 3 || len(parts[0]) != 255 || len(parts[1]) != 255 || len(parts[2]) != 1 {
		t.Fatalf("unexpected split lengths: %d %d %d", len(parts[0]), len(parts[1]), len(parts[2]))
	}
}

func TestFindLocation(t *testing.T) {
	plugin := &Redis{}
	zone := &Zone{
		Name: "example.org.",
		Locations: map[string]struct{}{
			"@":      {},
			"api":    {},
			"*":      {},
			"subdel": {},
			"*.deep": {},
		},
	}

	tests := []struct {
		name  string
		qname string
		want  string
	}{
		{name: "apex", qname: "example.org.", want: "@"},
		{name: "exact", qname: "api.example.org.", want: "api"},
		{name: "wildcard", qname: "foo.example.org.", want: "*"},
		{name: "deep wildcard", qname: "x.deep.example.org.", want: "*.deep"},
		{name: "delegation blocks wildcard", qname: "host.subdel.example.org.", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := plugin.findLocation(tc.qname, zone); got != tc.want {
				t.Fatalf("findLocation(%q) = %q, want %q", tc.qname, got, tc.want)
			}
		})
	}
}

func TestRecordEffectiveTTL(t *testing.T) {
	record := &Record{
		A:  []ARecord{{TTL: 300}},
		MX: []MXRecord{{TTL: 120}},
	}
	if ttl := record.effectiveTTL(600); ttl != 120 {
		t.Fatalf("effectiveTTL = %d, want 120", ttl)
	}
}

func TestDiskCachePutGetAndCleanup(t *testing.T) {
	cache, err := newDiskCache(cacheConfig{
		path:            t.TempDir(),
		maxEntries:      1,
		maxEntrySize:    1024,
		cleanupInterval: time.Nanosecond,
	})
	mustNoErr(t, err)

	now := time.Now()
	mustNoErr(t, cache.Put("example.org.", "a", `{"a":[{"ttl":300,"ip":"192.0.2.1"}]}`, time.Minute, now))
	payload, hit, err := cache.Get("example.org.", "a", now)
	mustNoErr(t, err)
	if !hit || payload == "" {
		t.Fatalf("cache hit = %t payload=%q", hit, payload)
	}

	mustNoErr(t, cache.Put("example.org.", "b", `{"a":[{"ttl":300,"ip":"192.0.2.2"}]}`, time.Minute, now))
	mustNoErr(t, cache.cleanup(time.Now().Add(time.Minute)))

	count := 0
	_ = filepath.WalkDir(cache.root, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			count++
		}
		return nil
	})
	if count > 1 {
		t.Fatalf("cache file count = %d, want <= 1", count)
	}
}

func TestDiskCacheCorruptionIsRejected(t *testing.T) {
	cache, err := newDiskCache(cacheConfig{
		path:            t.TempDir(),
		maxEntries:      4,
		maxEntrySize:    1024,
		cleanupInterval: time.Minute,
	})
	mustNoErr(t, err)

	filename := cache.entryPath("example.org.", "a")
	mustNoErr(t, os.MkdirAll(filepath.Dir(filename), 0o755))
	mustNoErr(t, os.WriteFile(filename, []byte("{broken"), 0o644))

	_, hit, err := cache.Get("example.org.", "a", time.Now())
	if hit || !errors.Is(err, errCorruptCache) {
		t.Fatalf("cache.Get() = hit %t err %v, want corrupt cache error", hit, err)
	}
}

func TestCacheEntryVersionRoundTrip(t *testing.T) {
	entry := cacheEntry{
		Version:   cacheFormatVersion,
		Zone:      "example.org.",
		Label:     "a",
		Payload:   `{}`,
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
		StoredAt:  time.Now().Unix(),
	}
	data, err := json.Marshal(entry)
	mustNoErr(t, err)
	var decoded cacheEntry
	mustNoErr(t, json.Unmarshal(data, &decoded))
	if decoded.Version != cacheFormatVersion || decoded.Zone != entry.Zone || decoded.Label != entry.Label {
		t.Fatalf("decoded entry = %+v, want %+v", decoded, entry)
	}
}

func TestPluginUsesReadOnlyRedisOperations(t *testing.T) {
	backend := defaultLookupBackend()
	plugin := newTestPlugin(t, backend)

	req := new(dns.Msg)
	req.SetQuestion("x.example.com.", dns.TypeA)
	resp := serveTestDNS(t, plugin, req)
	if resp.Rcode != dns.RcodeSuccess {
		t.Fatalf("rcode = %d, want success", resp.Rcode)
	}

	if backend.seenCommand("HSET") {
		t.Fatal("unexpected write command HSET")
	}
	if !backend.seenCommand("SCAN") || !backend.seenCommand("HKEYS") || !backend.seenCommand("HGET") {
		t.Fatalf("expected read-only commands SCAN/HKEYS/HGET, got %#v", backend.commands)
	}
}
