package redis

import (
	"strings"
	"testing"
	"time"

	"github.com/coredns/coredns/plugin/pkg/dnstest"
	"github.com/coredns/coredns/plugin/pkg/fall"
	"github.com/coredns/coredns/plugin/test"
	"github.com/gomodule/redigo/redis"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

type fakeRedisConn struct {
	records map[string]string
}

func (f *fakeRedisConn) Close() error { return nil }
func (f *fakeRedisConn) Err() error   { return nil }
func (f *fakeRedisConn) Send(commandName string, args ...interface{}) error {
	return nil
}
func (f *fakeRedisConn) Flush() error { return nil }
func (f *fakeRedisConn) Receive() (interface{}, error) {
	return nil, nil
}

func (f *fakeRedisConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	switch strings.ToUpper(commandName) {
	case "DBSIZE":
		return int64(1), nil
	case "HKEYS":
		keys := make([]interface{}, 0, len(f.records))
		for key := range f.records {
			keys = append(keys, []byte(key))
		}
		return keys, nil
	case "HGET":
		if len(args) < 2 {
			return nil, redis.ErrNil
		}
		record, ok := f.records[args[1].(string)]
		if !ok {
			return nil, redis.ErrNil
		}
		return []byte(record), nil
	}
	return nil, nil
}

type testNextHandler struct {
	called bool
}

func (t *testNextHandler) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	t.called = true
	m := new(dns.Msg)
	m.SetRcode(r, dns.RcodeRefused)
	_ = w.WriteMsg(m)
	return dns.RcodeRefused, nil
}

func (t *testNextHandler) Name() string { return "next" }

func newTestRedis(records map[string]string) *Redis {
	return &Redis{
		Pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return &fakeRedisConn{records: records}, nil
			},
		},
		Ttl:            300,
		Zones:          []string{"example.org."},
		LastZoneUpdate: time.Now(),
		lastKeyCount:   1,
	}
}

func TestFallthroughOnNameError(t *testing.T) {
	tests := []struct {
		name      string
		fall      fall.F
		wantNext  bool
		wantRcode int
	}{
		{
			name:      "disabled",
			wantRcode: dns.RcodeNameError,
		},
		{
			name:      "all zones",
			fall:      fall.Root,
			wantNext:  true,
			wantRcode: dns.RcodeRefused,
		},
		{
			name:      "matching zone",
			fall:      fall.F{Zones: []string{"example.org."}},
			wantNext:  true,
			wantRcode: dns.RcodeRefused,
		},
		{
			name:      "non-matching zone",
			fall:      fall.F{Zones: []string{"example.net."}},
			wantRcode: dns.RcodeNameError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := newTestRedis(map[string]string{
				"@": `{"soa":{"ttl":300,"minttl":100,"mbox":"hostmaster.example.org.","ns":"ns1.example.org.","refresh":44,"retry":55,"expire":66}}`,
			})
			r.Fall = tc.fall

			next := &testNextHandler{}
			r.Next = next

			req := new(dns.Msg)
			req.SetQuestion("missing.example.org.", dns.TypeA)
			rec := dnstest.NewRecorder(&test.ResponseWriter{})

			_, err := r.ServeDNS(context.TODO(), rec, req)
			if err != nil {
				t.Fatalf("ServeDNS returned error: %v", err)
			}
			if next.called != tc.wantNext {
				t.Fatalf("next handler called = %t, want %t", next.called, tc.wantNext)
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

func TestFallthroughDoesNotApplyToNODATA(t *testing.T) {
	r := newTestRedis(map[string]string{
		"txtonly": `{"txt":[{"ttl":300,"text":"hello"}]}`,
	})
	r.Fall = fall.Root

	next := &testNextHandler{}
	r.Next = next

	req := new(dns.Msg)
	req.SetQuestion("txtonly.example.org.", dns.TypeA)
	rec := dnstest.NewRecorder(&test.ResponseWriter{})

	_, err := r.ServeDNS(context.TODO(), rec, req)
	if err != nil {
		t.Fatalf("ServeDNS returned error: %v", err)
	}
	if next.called {
		t.Fatal("next handler was called for NODATA response")
	}
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
