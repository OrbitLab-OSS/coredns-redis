package redis

import (
	"errors"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"golang.org/x/net/context"
)

// ServeDNS implements the plugin.Handler interface.
func (r *Redis) ServeDNS(ctx context.Context, w dns.ResponseWriter, req *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: req}

	qname := normalizeQuestionName(state.Name())
	qtype := state.Type()

	zoneName := r.zones.Match(qname)
	if zoneName == "" {
		return plugin.NextOrFailure(qname, r.Next, ctx, w, req)
	}

	zone, err := r.zones.Get(zoneName)
	if err != nil || zone == nil {
		r.logRedisError("zone-load", "error loading redis zone %q: %v", zoneName, err)
		return r.errorResponse(state, dns.RcodeServerFailure, nil)
	}

	if qtype == "AXFR" {
		return r.serveAXFR(w, req, zone)
	}

	location := r.findLocation(qname, zone)
	if location == "" {
		if r.Fall.Through(qname) {
			return plugin.NextOrFailure(qname, r.Next, ctx, w, req)
		}
		return r.errorResponse(state, dns.RcodeNameError, nil)
	}

	record, err := r.lookupRecord(zone, qname, location)
	if err != nil {
		return r.errorResponse(state, dns.RcodeServerFailure, nil)
	}
	if record == nil {
		if r.Fall.Through(qname) {
			return plugin.NextOrFailure(qname, r.Next, ctx, w, req)
		}
		return r.errorResponse(state, dns.RcodeNameError, nil)
	}

	answers, extras, ok := r.answersForType(qtype, qname, zone, record)
	if !ok {
		return r.errorResponse(state, dns.RcodeNotImplemented, nil)
	}

	message := new(dns.Msg)
	message.SetReply(req)
	message.Authoritative = true
	message.RecursionAvailable = false
	message.Compress = true
	message.Answer = append(message.Answer, answers...)
	message.Extra = append(message.Extra, extras...)

	state.SizeAndDo(message)
	message = state.Scrub(message)
	_ = w.WriteMsg(message)
	return dns.RcodeSuccess, nil
}

func (r *Redis) lookupRecord(zone *Zone, qname, location string) (*Record, error) {
	record, err := r.getRecord(zone.Name, location)
	if err == nil {
		return record, nil
	}
	if !errors.Is(err, errRecordNotFound) {
		return nil, err
	}

	refreshedZone, refreshErr := r.zones.RefreshZone(zone.Name)
	if refreshErr == nil && refreshedZone != nil {
		location = r.findLocation(qname, refreshedZone)
		if location == "" {
			return nil, nil
		}
		return r.safeRecordLookup(refreshedZone.Name, location)
	}
	return nil, err
}

func (r *Redis) serveAXFR(w dns.ResponseWriter, req *dns.Msg, zone *Zone) (int, error) {
	records := r.AXFR(zone)

	ch := make(chan *dns.Envelope)
	transfer := new(dns.Transfer)

	go func() {
		start, currentLen := 0, 0
		for index, rr := range records {
			currentLen += dns.Len(rr)
			if currentLen > transferLength {
				ch <- &dns.Envelope{RR: records[start:index]}
				start = index
				currentLen = 0
			}
		}
		if start < len(records) {
			ch <- &dns.Envelope{RR: records[start:]}
		}
		close(ch)
	}()

	if err := transfer.Out(w, req, ch); err != nil {
		r.logRedisError("axfr", "error serving AXFR for zone %q: %v", zone.Name, err)
	}
	w.Hijack()
	return dns.RcodeSuccess, nil
}

func (r *Redis) errorResponse(state request.Request, rcode int, err error) (int, error) {
	message := new(dns.Msg)
	message.SetRcode(state.Req, rcode)
	message.Authoritative = true
	message.RecursionAvailable = false
	message.Compress = true

	state.SizeAndDo(message)
	_ = state.W.WriteMsg(message)
	return dns.RcodeSuccess, err
}
