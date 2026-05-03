package redis

import (
	"net"
	"time"
)

type Zone struct {
	Name        string
	Locations   map[string]struct{}
	RefreshedAt time.Time
}

type Record struct {
	A     []ARecord     `json:"a,omitempty"`
	AAAA  []AAAARecord  `json:"aaaa,omitempty"`
	TXT   []TXTRecord   `json:"txt,omitempty"`
	CNAME []CNAMERecord `json:"cname,omitempty"`
	NS    []NSRecord    `json:"ns,omitempty"`
	MX    []MXRecord    `json:"mx,omitempty"`
	SRV   []SRVRecord   `json:"srv,omitempty"`
	CAA   []CAARecord   `json:"caa,omitempty"`
	SOA   SOARecord     `json:"soa,omitempty"`
}

type ARecord struct {
	TTL uint32 `json:"ttl,omitempty"`
	IP  net.IP `json:"ip"`
}

type AAAARecord struct {
	TTL uint32 `json:"ttl,omitempty"`
	IP  net.IP `json:"ip"`
}

type TXTRecord struct {
	TTL  uint32 `json:"ttl,omitempty"`
	Text string `json:"text"`
}

type CNAMERecord struct {
	TTL  uint32 `json:"ttl,omitempty"`
	Host string `json:"host"`
}

type NSRecord struct {
	TTL  uint32 `json:"ttl,omitempty"`
	Host string `json:"host"`
}

type MXRecord struct {
	TTL        uint32 `json:"ttl,omitempty"`
	Host       string `json:"host"`
	Preference uint16 `json:"preference"`
}

type SRVRecord struct {
	TTL      uint32 `json:"ttl,omitempty"`
	Priority uint16 `json:"priority"`
	Weight   uint16 `json:"weight"`
	Port     uint16 `json:"port"`
	Target   string `json:"target"`
}

type SOARecord struct {
	TTL     uint32 `json:"ttl,omitempty"`
	NS      string `json:"ns"`
	MBox    string `json:"mbox"`
	Refresh uint32 `json:"refresh"`
	Retry   uint32 `json:"retry"`
	Expire  uint32 `json:"expire"`
	MinTTL  uint32 `json:"minttl"`
}

type CAARecord struct {
	TTL   uint32 `json:"ttl,omitempty"`
	Flag  uint8  `json:"flag"`
	Tag   string `json:"tag"`
	Value string `json:"value"`
}

func (r *Record) effectiveTTL(defaultTTL uint32) uint32 {
	if r == nil {
		return defaultTTL
	}

	ttl := uint32(0)
	update := func(candidate uint32) {
		candidate = effectiveTTL(defaultTTL, candidate)
		if ttl == 0 || candidate < ttl {
			ttl = candidate
		}
	}

	for _, record := range r.A {
		update(record.TTL)
	}
	for _, record := range r.AAAA {
		update(record.TTL)
	}
	for _, record := range r.TXT {
		update(record.TTL)
	}
	for _, record := range r.CNAME {
		update(record.TTL)
	}
	for _, record := range r.NS {
		update(record.TTL)
	}
	for _, record := range r.MX {
		update(record.TTL)
	}
	for _, record := range r.SRV {
		update(record.TTL)
	}
	for _, record := range r.CAA {
		update(record.TTL)
	}
	if r.SOA.NS != "" || r.SOA.MBox != "" || r.SOA.Refresh != 0 || r.SOA.Retry != 0 || r.SOA.Expire != 0 || r.SOA.MinTTL != 0 {
		update(r.SOA.TTL)
	}

	if ttl == 0 {
		return defaultTTL
	}
	return ttl
}
