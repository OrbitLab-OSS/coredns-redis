package redis

import (
	"errors"
	"testing"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/pkg/fall"
	"github.com/coredns/coredns/plugin/test"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

type testNextHandler struct {
	called bool
}

func (t *testNextHandler) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	t.called = true
	msg := new(dns.Msg)
	msg.SetRcode(r, dns.RcodeRefused)
	_ = w.WriteMsg(msg)
	return dns.RcodeRefused, nil
}

func (t *testNextHandler) Name() string { return "next" }

func TestFallthroughOnNameError(t *testing.T) {
	tests := []struct {
		name      string
		fall      fall.F
		wantNext  bool
		wantRcode int
	}{
		{name: "disabled", wantRcode: dns.RcodeNameError},
		{name: "all-zones", fall: fall.Root, wantNext: true, wantRcode: dns.RcodeRefused},
		{name: "matching-zone", fall: fall.F{Zones: []string{"example.org."}}, wantNext: true, wantRcode: dns.RcodeRefused},
		{name: "non-matching-zone", fall: fall.F{Zones: []string{"example.net."}}, wantRcode: dns.RcodeNameError},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.setZone("example.org.", map[string]string{
				"@": `{"soa":{"ttl":300,"minttl":100,"mbox":"hostmaster.example.org.","ns":"ns1.example.org.","refresh":44,"retry":55,"expire":66}}`,
			})

			plugin := newTestPlugin(t, backend)
			plugin.Fall = tc.fall

			next := &testNextHandler{}
			plugin.Next = next

			req := new(dns.Msg)
			req.SetQuestion("missing.example.org.", dns.TypeA)
			rec := dnstest.NewRecorder(&test.ResponseWriter{})

			_, err := plugin.ServeDNS(context.TODO(), rec, req)
			mustNoErr(t, err)
			if next.called != tc.wantNext {
				t.Fatalf("next.called = %t, want %t", next.called, tc.wantNext)
			}
			if rec.Msg == nil {
				t.Fatal("expected response message")
			}
			if rec.Msg.Rcode != tc.wantRcode {
				t.Fatalf("rcode = %d, want %d", rec.Msg.Rcode, tc.wantRcode)
			}
		})
	}
}

func TestFallthroughDoesNotApplyToNoData(t *testing.T) {
	backend := newFakeBackend()
	backend.setZone("example.org.", map[string]string{
		"txtonly": `{"txt":[{"ttl":300,"text":"hello"}]}`,
	})

	plugin := newTestPlugin(t, backend)
	plugin.Fall = fall.Root
	plugin.Next = &testNextHandler{}

	req := new(dns.Msg)
	req.SetQuestion("txtonly.example.org.", dns.TypeA)
	rec := dnstest.NewRecorder(&test.ResponseWriter{})

	_, err := plugin.ServeDNS(context.TODO(), rec, req)
	mustNoErr(t, err)
	if rec.Msg == nil {
		t.Fatal("expected response message")
	}
	if rec.Msg.Rcode != dns.RcodeSuccess {
		t.Fatalf("rcode = %d, want %d", rec.Msg.Rcode, dns.RcodeSuccess)
	}
	if len(rec.Msg.Answer) != 0 {
		t.Fatalf("answer count = %d, want 0", len(rec.Msg.Answer))
	}
}

func TestRedisUnavailableWithoutCacheReturnsServfail(t *testing.T) {
	backend := newFakeBackend()
	backend.setZone("example.org.", map[string]string{
		"api": `{"a":[{"ttl":300,"ip":"192.0.2.1"}]}`,
	})
	backend.getRecordErr = errors.New("redis down")

	plugin := newTestPlugin(t, backend)

	req := new(dns.Msg)
	req.SetQuestion("api.example.org.", dns.TypeA)
	resp := serveTestDNS(t, plugin, req)
	if resp.Rcode != dns.RcodeServerFailure {
		t.Fatalf("rcode = %d, want %d", resp.Rcode, dns.RcodeServerFailure)
	}
}
