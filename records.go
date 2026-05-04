package redis

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/miekg/dns"
)

var (
	errCorruptCache   = errors.New("cache entry is corrupt")
	errRecordTooLarge = errors.New("redis record exceeds size limit")
)

type rrBuilder func(name string, z *Zone, record *Record) (answers, extras []dns.RR)

func (r *Redis) getRecord(zone, label string) (*Record, error) {
	now := time.Now()

	if r.cache != nil {
		payload, hit, err := r.cache.Get(zone, label, now)
		if err != nil && !errors.Is(err, errCorruptCache) {
			r.logCacheError("cache-read", "error reading cache entry for zone=%q label=%q: %v", zone, label, err)
		}
		if hit {
			record, err := decodeRecord(payload, r.maxRecordSize)
			if err == nil {
				r.metrics.addCacheHit()
				return record, nil
			}
			r.logCacheError("cache-decode", "error decoding cached record for zone=%q label=%q: %v", zone, label, err)
		}
		r.metrics.addCacheMiss()
	}

	payload, err := r.backend.GetRecord(zone, label)
	if err != nil {
		if errors.Is(err, errRecordNotFound) {
			return nil, err
		}
		r.logRedisError("redis-read", "error reading redis record for zone=%q label=%q: %v", zone, label, err)
		return nil, err
	}
	r.metrics.addRedisRead()

	record, err := decodeRecord(payload, r.maxRecordSize)
	if err != nil {
		r.logRedisError("redis-decode", "error decoding redis record for zone=%q label=%q: %v", zone, label, err)
		return nil, err
	}

	if r.cache != nil {
		ttl := time.Duration(record.effectiveTTL(r.defaultTTL)) * time.Second
		if err := r.cache.Put(zone, label, payload, ttl, now); err != nil {
			r.logCacheError("cache-write", "error writing cache entry for zone=%q label=%q: %v", zone, label, err)
		}
	}

	return record, nil
}

func decodeRecord(payload string, maxRecordSize int) (*Record, error) {
	if len(payload) == 0 {
		return nil, errRecordNotFound
	}
	if len(payload) > maxRecordSize {
		return nil, errRecordTooLarge
	}

	record := new(Record)
	if err := json.Unmarshal([]byte(payload), record); err != nil {
		return nil, err
	}
	return record, nil
}

func (r *Redis) A(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.A {
		ip := current.IP.To4()
		if ip == nil {
			continue
		}
		answer := new(dns.A)
		answer.Hdr = r.answerHeader(name, dns.TypeA, current.TTL)
		answer.A = ip
		answers = append(answers, answer)
	}
	return answers, nil
}

func (r *Redis) AAAA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.AAAA {
		if current.IP == nil || current.IP.To4() != nil || current.IP.To16() == nil {
			continue
		}
		answer := new(dns.AAAA)
		answer.Hdr = r.answerHeader(name, dns.TypeAAAA, current.TTL)
		answer.AAAA = current.IP
		answers = append(answers, answer)
	}
	return answers, nil
}

func (r *Redis) CNAME(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.CNAME {
		target, ok := absoluteName(current.Host, z.Name)
		if !ok {
			continue
		}
		answer := new(dns.CNAME)
		answer.Hdr = r.answerHeader(name, dns.TypeCNAME, current.TTL)
		answer.Target = target
		answers = append(answers, answer)
	}
	return answers, nil
}

func (r *Redis) TXT(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.TXT {
		if current.Text == "" {
			continue
		}
		answer := new(dns.TXT)
		answer.Hdr = r.answerHeader(name, dns.TypeTXT, current.TTL)
		answer.Txt = split255(current.Text)
		answers = append(answers, answer)
	}
	return answers, nil
}

func (r *Redis) NS(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.NS {
		target, ok := absoluteName(current.Host, z.Name)
		if !ok {
			continue
		}
		answer := new(dns.NS)
		answer.Hdr = r.answerHeader(name, dns.TypeNS, current.TTL)
		answer.Ns = target
		answers = append(answers, answer)
		extras = append(extras, r.additionalAddressRecords(target, z)...)
	}
	return answers, extras
}

func (r *Redis) MX(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.MX {
		target, ok := absoluteName(current.Host, z.Name)
		if !ok {
			continue
		}
		answer := new(dns.MX)
		answer.Hdr = r.answerHeader(name, dns.TypeMX, current.TTL)
		answer.Mx = target
		answer.Preference = current.Preference
		answers = append(answers, answer)
		extras = append(extras, r.additionalAddressRecords(target, z)...)
	}
	return answers, extras
}

func (r *Redis) SRV(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.SRV {
		target, ok := absoluteName(current.Target, z.Name)
		if !ok {
			continue
		}
		answer := new(dns.SRV)
		answer.Hdr = r.answerHeader(name, dns.TypeSRV, current.TTL)
		answer.Target = target
		answer.Priority = current.Priority
		answer.Weight = current.Weight
		answer.Port = current.Port
		answers = append(answers, answer)
		extras = append(extras, r.additionalAddressRecords(target, z)...)
	}
	return answers, extras
}

func (r *Redis) SOA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	answer := new(dns.SOA)
	if record == nil || record.SOA.NS == "" {
		answer.Hdr = dns.RR_Header{
			Name:   dns.Fqdn(z.Name),
			Rrtype: dns.TypeSOA,
			Class:  dns.ClassINET,
			Ttl:    effectiveTTL(r.defaultTTL, 0),
		}
		answer.Ns = dns.Fqdn("ns1." + strings.TrimSuffix(z.Name, "."))
		answer.Mbox = dns.Fqdn(hostmaster + "." + strings.TrimSuffix(z.Name, "."))
		answer.Refresh = 86400
		answer.Retry = 7200
		answer.Expire = 3600
		answer.Minttl = effectiveTTL(r.defaultTTL, 0)
	} else {
		ns, ok := absoluteName(record.SOA.NS, z.Name)
		if !ok {
			return nil, nil
		}
		mbox, ok := absoluteName(record.SOA.MBox, z.Name)
		if !ok {
			return nil, nil
		}
		answer.Hdr = r.answerHeader(z.Name, dns.TypeSOA, record.SOA.TTL)
		answer.Ns = ns
		answer.Mbox = mbox
		answer.Refresh = record.SOA.Refresh
		answer.Retry = record.SOA.Retry
		answer.Expire = record.SOA.Expire
		answer.Minttl = effectiveTTL(r.defaultTTL, record.SOA.MinTTL)
	}
	answer.Serial = r.serial()
	return []dns.RR{answer}, nil
}

func (r *Redis) CAA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return nil, nil
	}
	for _, current := range record.CAA {
		if current.Tag == "" || current.Value == "" {
			continue
		}
		answer := new(dns.CAA)
		answer.Hdr = r.answerHeader(name, dns.TypeCAA, current.TTL)
		answer.Flag = current.Flag
		answer.Tag = current.Tag
		answer.Value = current.Value
		answers = append(answers, answer)
	}
	return answers, nil
}

func (r *Redis) AXFR(z *Zone) []dns.RR {
	builders := []rrBuilder{r.A, r.AAAA, r.CNAME, r.TXT, r.NS, r.MX, r.SRV, r.CAA}
	records := make([]dns.RR, 0, len(z.Locations)*2)

	apexRecord, _ := r.safeRecordLookup(z.Name, "@")
	soa, _ := r.SOA(z.Name, z, apexRecord)
	records = append(records, soa...)

	for _, label := range sortedLocations(z.Locations) {
		record, err := r.safeRecordLookup(z.Name, label)
		if err != nil || record == nil {
			continue
		}

		name := fqdnForLocation(label, z.Name)
		for _, build := range builders {
			answers, extras := build(name, z, record)
			records = append(records, answers...)
			records = append(records, extras...)
		}
	}

	records = append(records, soa...)
	return records
}

func (r *Redis) additionalAddressRecords(name string, z *Zone) []dns.RR {
	label, ok := relativeNameForZone(name, z.Name)
	if !ok {
		return nil
	}

	record, err := r.safeRecordLookup(z.Name, label)
	if err != nil || record == nil {
		return nil
	}

	answers := make([]dns.RR, 0, 4)
	aAnswers, _ := r.A(name, z, record)
	answers = append(answers, aAnswers...)
	aaaaAnswers, _ := r.AAAA(name, z, record)
	answers = append(answers, aaaaAnswers...)
	return answers
}

func (r *Redis) safeRecordLookup(zone, label string) (*Record, error) {
	record, err := r.getRecord(zone, label)
	if err != nil {
		if errors.Is(err, errRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return record, nil
}

func (r *Redis) answerHeader(name string, rrType uint16, ttl uint32) dns.RR_Header {
	return dns.RR_Header{
		Name:   dns.Fqdn(name),
		Rrtype: rrType,
		Class:  dns.ClassINET,
		Ttl:    effectiveTTL(r.defaultTTL, ttl),
	}
}

func effectiveTTL(defaultTTL, ttl uint32) uint32 {
	if ttl != 0 {
		return ttl
	}
	if defaultTTL != 0 {
		return defaultTTL
	}
	return defaultTtl
}

func absoluteName(name, zone string) (string, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", false
	}
	candidate := dns.Fqdn(name)
	normalizedZone := normalizeZone(zone)

	switch {
	case strings.HasSuffix(name, "."):
		name = candidate
	case strings.HasSuffix(strings.ToLower(candidate), strings.ToLower(normalizedZone)):
		name = candidate
	default:
		name = dns.Fqdn(name + "." + strings.TrimSuffix(normalizedZone, "."))
	}
	return name, isDomainName(name)
}

func relativeNameForZone(name, zone string) (string, bool) {
	name = dns.Fqdn(name)
	zone = normalizeZone(zone)
	if !strings.HasSuffix(strings.ToLower(name), strings.ToLower(zone)) {
		return "", false
	}
	if strings.EqualFold(name, zone) {
		return "@", true
	}

	relative := strings.TrimSuffix(name, zone)
	relative = strings.TrimSuffix(relative, ".")
	if relative == "" {
		return "@", true
	}
	return relative, true
}

func fqdnForLocation(label, zone string) string {
	if label == "@" {
		return normalizeZone(zone)
	}
	name, ok := absoluteName(label, zone)
	if !ok {
		return normalizeZone(zone)
	}
	return name
}

func isValidLocation(label, zone string) bool {
	if label == "@" {
		return true
	}
	_, ok := absoluteName(label, zone)
	return ok
}

func isDomainName(name string) bool {
	if name == "" {
		return false
	}
	_, ok := dns.IsDomainName(dns.Fqdn(name))
	return ok
}

func sortedLocations(locations map[string]struct{}) []string {
	values := make([]string, 0, len(locations))
	for value := range locations {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func normalizeZone(zone string) string {
	if zone == "" {
		return ""
	}
	return strings.ToLower(dns.Fqdn(zone))
}

func normalizeQuestionName(name string) string {
	return strings.ToLower(dns.Fqdn(name))
}
