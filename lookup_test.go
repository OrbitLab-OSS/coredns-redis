package redis

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/test"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

func TestLookupResponses(t *testing.T) {
	tests := []struct {
		name string
		tc   test.Case
	}{
		{
			name: "A",
			tc: test.Case{
				Qname: "x.example.com.", Qtype: dns.TypeA,
				Answer: []dns.RR{
					test.A("x.example.com. 300 IN A 1.2.3.4"),
					test.A("x.example.com. 300 IN A 5.6.7.8"),
				},
			},
		},
		{
			name: "AAAA",
			tc: test.Case{
				Qname: "x.example.com.", Qtype: dns.TypeAAAA,
				Answer: []dns.RR{
					test.AAAA("x.example.com. 300 IN AAAA ::1"),
				},
			},
		},
		{
			name: "TXT",
			tc: test.Case{
				Qname: "x.example.com.", Qtype: dns.TypeTXT,
				Answer: []dns.RR{
					test.TXT("x.example.com. 300 IN TXT bar"),
					test.TXT("x.example.com. 300 IN TXT foo"),
				},
			},
		},
		{
			name: "CNAME",
			tc: test.Case{
				Qname: "y.example.com.", Qtype: dns.TypeCNAME,
				Answer: []dns.RR{
					test.CNAME("y.example.com. 300 IN CNAME x.example.com."),
				},
			},
		},
		{
			name: "NS with extras",
			tc: test.Case{
				Qname: "x.example.com.", Qtype: dns.TypeNS,
				Answer: []dns.RR{
					test.NS("x.example.com. 300 IN NS ns1.example.com."),
					test.NS("x.example.com. 300 IN NS ns2.example.com."),
				},
				Extra: []dns.RR{
					test.A("ns1.example.com. 300 IN A 2.2.2.2"),
					test.A("ns2.example.com. 300 IN A 3.3.3.3"),
				},
			},
		},
		{
			name: "MX with extras",
			tc: test.Case{
				Qname: "x.example.com.", Qtype: dns.TypeMX,
				Answer: []dns.RR{
					test.MX("x.example.com. 300 IN MX 10 mx1.example.com."),
					test.MX("x.example.com. 300 IN MX 10 mx2.example.com."),
				},
			},
		},
		{
			name: "SRV with extras",
			tc: test.Case{
				Qname: "_sip._tcp.example.com.", Qtype: dns.TypeSRV,
				Answer: []dns.RR{
					test.SRV("_sip._tcp.example.com. 300 IN SRV 10 100 555 sip.example.com."),
				},
				Extra: []dns.RR{
					test.A("sip.example.com. 300 IN A 7.7.7.7"),
					test.AAAA("sip.example.com. 300 IN AAAA ::1"),
				},
			},
		},
		{
			name: "NXDOMAIN",
			tc: test.Case{
				Qname: "notexists.example.com.", Qtype: dns.TypeA,
				Rcode: dns.RcodeNameError,
			},
		},
		{
			name: "wildcard MX",
			tc: test.Case{
				Qname: "host3.example.net.", Qtype: dns.TypeMX,
				Answer: []dns.RR{
					test.MX("host3.example.net. 300 IN MX 10 host1.example.net."),
				},
				Extra: []dns.RR{
					test.A("host1.example.net. 300 IN A 5.5.5.5"),
				},
			},
		},
		{
			name: "wildcard TXT",
			tc: test.Case{
				Qname: "foo.bar.example.net.", Qtype: dns.TypeTXT,
				Answer: []dns.RR{
					test.TXT("foo.bar.example.net. 300 IN TXT \"this is a wildcard\""),
				},
			},
		},
		{
			name: "literal star is exact only",
			tc: test.Case{
				Qname: "sub.*.example.net.", Qtype: dns.TypeTXT,
				Answer: []dns.RR{
					test.TXT("sub.*.example.net. 300 IN TXT \"literal star\""),
				},
			},
		},
		{
			name: "delegation blocks wildcard",
			tc: test.Case{
				Qname: "host.subdel.example.net.", Qtype: dns.TypeA,
				Rcode: dns.RcodeNameError,
			},
		},
		{
			name: "malformed redis record",
			tc: test.Case{
				Qname: "host1.example.test.", Qtype: dns.TypeA,
				Rcode: dns.RcodeServerFailure,
			},
		},
		{
			name: "CAA",
			tc: test.Case{
				Qname: "caa.example.com.", Qtype: dns.TypeCAA,
				Answer: []dns.RR{
					test.CAA("caa.example.com. 180 IN CAA 0 issue \"letsencrypt.org\""),
				},
			},
		},
	}

	plugin := newTestPlugin(t, defaultLookupBackend())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := serveTestDNS(t, plugin, tt.tc.Msg())
			if err := test.SortAndCheck(resp, tt.tc); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFQDNNormalizationAndRelativeTargets(t *testing.T) {
	plugin := newTestPlugin(t, defaultLookupBackend())

	req := new(dns.Msg)
	req.SetQuestion("REL.Example.Com", dns.TypeMX)
	resp := serveTestDNS(t, plugin, req)

	tc := test.Case{
		Qname: "rel.example.com.", Qtype: dns.TypeMX,
		Answer: []dns.RR{
			test.MX("rel.example.com. 120 IN MX 10 mail.example.com."),
		},
		Extra: []dns.RR{
			test.A("mail.example.com. 300 IN A 9.9.9.9"),
		},
	}
	if err := test.SortAndCheck(resp, tc); err != nil {
		t.Fatal(err)
	}
}

func TestQualifiedTargetWithoutTrailingDotIsNotRebased(t *testing.T) {
	plugin := newTestPlugin(t, defaultLookupBackend())

	req := new(dns.Msg)
	req.SetQuestion("qualified.example.com.", dns.TypeSRV)
	resp := serveTestDNS(t, plugin, req)

	tc := test.Case{
		Qname: "qualified.example.com.", Qtype: dns.TypeSRV,
		Answer: []dns.RR{
			test.SRV("qualified.example.com. 120 IN SRV 10 5 8443 srv1.example.com."),
		},
		Extra: []dns.RR{
			test.A("srv1.example.com. 300 IN A 10.10.10.10"),
		},
	}
	if err := test.SortAndCheck(resp, tc); err != nil {
		t.Fatal(err)
	}
}

func TestLookupSOAResponse(t *testing.T) {
	plugin := newTestPlugin(t, defaultLookupBackend())

	req := new(dns.Msg)
	req.SetQuestion("example.com.", dns.TypeSOA)
	resp := serveTestDNS(t, plugin, req)
	if resp.Rcode != dns.RcodeSuccess {
		t.Fatalf("rcode = %d, want %d", resp.Rcode, dns.RcodeSuccess)
	}
	if len(resp.Answer) != 1 {
		t.Fatalf("answer count = %d, want 1", len(resp.Answer))
	}

	soa, ok := resp.Answer[0].(*dns.SOA)
	if !ok {
		t.Fatalf("answer type = %T, want *dns.SOA", resp.Answer[0])
	}
	if soa.Hdr.Name != "example.com." || soa.Hdr.Ttl != 300 {
		t.Fatalf("unexpected SOA header: %+v", soa.Hdr)
	}
	if soa.Ns != "ns1.example.com." || soa.Mbox != "hostmaster.example.com." {
		t.Fatalf("unexpected SOA names: %+v", soa)
	}
}

func TestCacheHitAndRedisFailureFallback(t *testing.T) {
	backend := newFakeBackend()
	backend.setZone("example.org.", map[string]string{
		"api": `{"a":[{"ttl":300,"ip":"192.0.2.10"}]}`,
	})
	plugin := newTestPlugin(t, backend)

	req := new(dns.Msg)
	req.SetQuestion("api.example.org.", dns.TypeA)
	first := serveTestDNS(t, plugin, req)
	if first.Rcode != dns.RcodeSuccess {
		t.Fatalf("first rcode = %d, want success", first.Rcode)
	}

	_, _, beforeReads := backend.callCounts()
	backend.getRecordErr = errors.New("redis down")

	second := serveTestDNS(t, plugin, req)
	if second.Rcode != dns.RcodeSuccess {
		t.Fatalf("second rcode = %d, want success", second.Rcode)
	}
	_, _, afterReads := backend.callCounts()
	if afterReads != beforeReads {
		t.Fatalf("getRecordCalls = %d after cache hit, want %d", afterReads, beforeReads)
	}
}

func TestCacheExpirationRefreshesFromRedis(t *testing.T) {
	backend := newFakeBackend()
	backend.setZone("example.org.", map[string]string{
		"api": `{"a":[{"ttl":1,"ip":"192.0.2.10"}]}`,
	})
	plugin := newTestPlugin(t, backend)

	req := new(dns.Msg)
	req.SetQuestion("api.example.org.", dns.TypeA)
	serveTestDNS(t, plugin, req)

	cachePath := plugin.cache.entryPath("example.org.", "api")
	data, err := os.ReadFile(cachePath)
	mustNoErr(t, err)

	var entry cacheEntry
	mustNoErr(t, json.Unmarshal(data, &entry))
	entry.ExpiresAt = time.Now().Add(-time.Minute).Unix()
	updated, err := json.Marshal(entry)
	mustNoErr(t, err)
	mustNoErr(t, os.WriteFile(cachePath, updated, 0o644))

	backend.setRecord("example.org.", "api", `{"a":[{"ttl":300,"ip":"192.0.2.20"}]}`)
	resp := serveTestDNS(t, plugin, req)

	tc := test.Case{
		Qname: "api.example.org.", Qtype: dns.TypeA,
		Answer: []dns.RR{test.A("api.example.org. 300 IN A 192.0.2.20")},
	}
	if err := test.SortAndCheck(resp, tc); err != nil {
		t.Fatal(err)
	}
}

func TestCacheCorruptionRecovery(t *testing.T) {
	backend := newFakeBackend()
	backend.setZone("example.org.", map[string]string{
		"api": `{"a":[{"ttl":300,"ip":"192.0.2.10"}]}`,
	})
	plugin := newTestPlugin(t, backend)

	req := new(dns.Msg)
	req.SetQuestion("api.example.org.", dns.TypeA)
	serveTestDNS(t, plugin, req)

	cachePath := plugin.cache.entryPath("example.org.", "api")
	mustNoErr(t, os.WriteFile(cachePath, []byte("{not-json"), 0o644))
	backend.setRecord("example.org.", "api", `{"a":[{"ttl":300,"ip":"192.0.2.30"}]}`)

	resp := serveTestDNS(t, plugin, req)
	tc := test.Case{
		Qname: "api.example.org.", Qtype: dns.TypeA,
		Answer: []dns.RR{test.A("api.example.org. 300 IN A 192.0.2.30")},
	}
	if err := test.SortAndCheck(resp, tc); err != nil {
		t.Fatal(err)
	}
}

func TestMissingRedisFieldReturnsNameError(t *testing.T) {
	backend := newFakeBackend()
	backend.setZoneLocations("example.org.", "ghost")

	plugin := newTestPlugin(t, backend)
	req := new(dns.Msg)
	req.SetQuestion("ghost.example.org.", dns.TypeA)
	resp := serveTestDNS(t, plugin, req)
	if resp.Rcode != dns.RcodeNameError {
		t.Fatalf("rcode = %d, want %d", resp.Rcode, dns.RcodeNameError)
	}
}

func TestConcurrentLookups(t *testing.T) {
	plugin := newTestPlugin(t, defaultLookupBackend())

	req := new(dns.Msg)
	req.SetQuestion("x.example.com.", dns.TypeA)

	errCh := make(chan error, 32)
	for i := 0; i < 32; i++ {
		go func() {
			resp := serveTestDNS(t, plugin, req.Copy())
			if resp.Rcode != dns.RcodeSuccess || len(resp.Answer) != 2 {
				errCh <- errors.New("unexpected lookup result")
				return
			}
			errCh <- nil
		}()
	}

	for i := 0; i < 32; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func TestAXFRIncludesZoneRecords(t *testing.T) {
	plugin := newTestPlugin(t, defaultLookupBackend())
	zone, err := plugin.zones.Get("example.com.")
	mustNoErr(t, err)

	records := plugin.AXFR(zone)
	if len(records) == 0 {
		t.Fatal("AXFR returned no records")
	}
	first, ok := records[0].(*dns.SOA)
	if !ok {
		t.Fatalf("first RR = %T, want *dns.SOA", records[0])
	}
	last, ok := records[len(records)-1].(*dns.SOA)
	if !ok {
		t.Fatalf("last RR = %T, want *dns.SOA", records[len(records)-1])
	}
	if first.Ns != "ns1.example.com." || last.Ns != "ns1.example.com." {
		t.Fatalf("unexpected SOA records: first=%+v last=%+v", first, last)
	}
}

func serveTestDNS(t *testing.T, plugin *Redis, req *dns.Msg) *dns.Msg {
	t.Helper()

	rec := dnstest.NewRecorder(&test.ResponseWriter{})
	_, err := plugin.ServeDNS(context.TODO(), rec, req)
	mustNoErr(t, err)

	if rec.Msg == nil {
		return new(dns.Msg)
	}
	return rec.Msg
}
