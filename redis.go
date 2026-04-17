package redis

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"

	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/fall"
	clog "github.com/coredns/coredns/plugin/pkg/log"

	redisCon "github.com/gomodule/redigo/redis"
)

var log = clog.NewWithPlugin("redis")

type Redis struct {
	Next           plugin.Handler
	Fall           fall.F
	Pool           *redisCon.Pool
	redisAddress   string
	redisPassword  string
	connectTimeout int
	readTimeout    int
	keyPrefix      string
	keySuffix      string
	Ttl            uint32
	Zones          []string
	LastZoneUpdate time.Time
	lastKeyCount   int
}

func (redis *Redis) KeyCount() int {
	var (
		reply interface{}
		err   error
	)
	log.Debug("getting redis database key count")
	conn := redis.Pool.Get()
	if conn == nil {
		log.Error("error connecting to redis")
		return -1
	}
	defer conn.Close()
	reply, err = conn.Do("DBSIZE")
	if err != nil {
		log.Error("error getting dbsize from redis:", err)
		return -1
	}
	dbsize, err := redisCon.Int(reply, nil)
	if err != nil {
		log.Error("error parsing dbsize:", err)
		return -1
	}
	log.Debugf("redis database key count: %d", dbsize)
	return dbsize
}

type RedisScanReply struct {
	cursor int
	keys   []string
}

func (redis *Redis) LoadZones() {
	var (
		reply interface{}
		err   error
		zones []string
	)

	log.Debug("loading zones")

	conn := redis.Pool.Get()
	if conn == nil {
		log.Error("error connecting to redis")
		return
	}
	defer conn.Close()

	matchPattern := redis.keyPrefix + "*" + redis.keySuffix

	/*
		SCAN is a cursor based iterator. This means that at every call of the command,
		the server returns an updated cursor that the user needs to use as the cursor
		argument in the next call.
		https://redis.io/docs/latest/commands/scan
	*/
	cursor := 0
	cursorBatchSize := 1000
	keysSeen := map[string]bool{}
	for {
		log.Debugf("scanning redis zones: cursor=%d match=%q count=%d", cursor, matchPattern, cursorBatchSize)
		reply, err = conn.Do("SCAN", cursor, "MATCH", matchPattern, "COUNT", cursorBatchSize)
		if err != nil {
			log.Errorf("error scanning redis zones with match %q: %v", matchPattern, err)
			return
		}

		scanReply, err := decodeScanReply(reply)
		if err != nil {
			log.Errorf("error parsing redis SCAN reply: %v", err)
			return
		}
		cursor = scanReply.cursor
		log.Debugf("redis zone scan batch returned %d keys; next cursor=%d", len(scanReply.keys), cursor)

		for _, zone := range scanReply.keys {
			// Note: a given element may be returned multiple times. It is up to
			// the application to handle the case of duplicated elements
			if _, found := keysSeen[zone]; !found {

				zone = strings.TrimPrefix(zone, redis.keyPrefix)
				zone = strings.TrimPrefix(zone, redis.keySuffix)

				zones = append(zones, zone)
				keysSeen[zone] = true
				log.Debugf("discovered redis zone: %s", zone)
			}
		}

		// Cursor will be 0 after all keys have been read
		if cursor == 0 {
			break
		}
	}

	redis.LastZoneUpdate = time.Now()
	redis.lastKeyCount = redis.KeyCount()
	redis.Zones = zones
	log.Debugf("loaded %d redis zones", len(zones))
}

func (redis *Redis) A(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, a := range record.A {
		if a.Ip == nil {
			continue
		}
		r := new(dns.A)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeA,
			Class: dns.ClassINET, Ttl: redis.minTtl(a.Ttl)}
		r.A = a.Ip
		answers = append(answers, r)
	}
	return
}

func (redis Redis) AAAA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, aaaa := range record.AAAA {
		if aaaa.Ip == nil {
			continue
		}
		r := new(dns.AAAA)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeAAAA,
			Class: dns.ClassINET, Ttl: redis.minTtl(aaaa.Ttl)}
		r.AAAA = aaaa.Ip
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) CNAME(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, cname := range record.CNAME {
		if len(cname.Host) == 0 {
			continue
		}
		r := new(dns.CNAME)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeCNAME,
			Class: dns.ClassINET, Ttl: redis.minTtl(cname.Ttl)}
		r.Target = dns.Fqdn(cname.Host)
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) TXT(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, txt := range record.TXT {
		if len(txt.Text) == 0 {
			continue
		}
		r := new(dns.TXT)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeTXT,
			Class: dns.ClassINET, Ttl: redis.minTtl(txt.Ttl)}
		r.Txt = split255(txt.Text)
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) NS(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, ns := range record.NS {
		if len(ns.Host) == 0 {
			continue
		}
		r := new(dns.NS)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeNS,
			Class: dns.ClassINET, Ttl: redis.minTtl(ns.Ttl)}
		r.Ns = ns.Host
		answers = append(answers, r)
		extras = append(extras, redis.hosts(ns.Host, z)...)
	}
	return
}

func (redis *Redis) MX(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, mx := range record.MX {
		if len(mx.Host) == 0 {
			continue
		}
		r := new(dns.MX)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeMX,
			Class: dns.ClassINET, Ttl: redis.minTtl(mx.Ttl)}
		r.Mx = mx.Host
		r.Preference = mx.Preference
		answers = append(answers, r)
		extras = append(extras, redis.hosts(mx.Host, z)...)
	}
	return
}

func (redis *Redis) SRV(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, srv := range record.SRV {
		if len(srv.Target) == 0 {
			continue
		}
		r := new(dns.SRV)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeSRV,
			Class: dns.ClassINET, Ttl: redis.minTtl(srv.Ttl)}
		r.Target = srv.Target
		r.Weight = srv.Weight
		r.Port = srv.Port
		r.Priority = srv.Priority
		answers = append(answers, r)
		extras = append(extras, redis.hosts(srv.Target, z)...)
	}
	return
}

func (redis *Redis) SOA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	r := new(dns.SOA)
	if record.SOA.Ns == "" {
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeSOA,
			Class: dns.ClassINET, Ttl: redis.Ttl}
		r.Ns = "ns1." + name
		r.Mbox = "hostmaster." + name
		r.Refresh = 86400
		r.Retry = 7200
		r.Expire = 3600
		r.Minttl = redis.Ttl
	} else {
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(z.Name), Rrtype: dns.TypeSOA,
			Class: dns.ClassINET, Ttl: redis.minTtl(record.SOA.Ttl)}
		r.Ns = record.SOA.Ns
		r.Mbox = record.SOA.MBox
		r.Refresh = record.SOA.Refresh
		r.Retry = record.SOA.Retry
		r.Expire = record.SOA.Expire
		r.Minttl = record.SOA.MinTtl
	}
	r.Serial = redis.serial()
	answers = append(answers, r)
	return
}

func (redis *Redis) CAA(name string, z *Zone, record *Record) (answers, extras []dns.RR) {
	if record == nil {
		return
	}
	for _, caa := range record.CAA {
		if caa.Value == "" || caa.Tag == "" {
			continue
		}
		r := new(dns.CAA)
		r.Hdr = dns.RR_Header{Name: dns.Fqdn(name), Rrtype: dns.TypeCAA, Class: dns.ClassINET}
		r.Flag = caa.Flag
		r.Tag = caa.Tag
		r.Value = caa.Value
		answers = append(answers, r)
	}
	return
}

func (redis *Redis) AXFR(z *Zone) (records []dns.RR) {
	log.Debugf("building AXFR response for zone %s with %d locations", z.Name, len(z.Locations))
	//soa, _ := redis.SOA(z.Name, z, record)
	soa := make([]dns.RR, 0)
	answers := make([]dns.RR, 0, 10)
	extras := make([]dns.RR, 0, 10)

	// Allocate slices for rr Records
	records = append(records, soa...)
	for key := range z.Locations {
		if key == "@" {
			location := redis.findLocation(z.Name, z)
			record := redis.get(location, z)
			soa, _ = redis.SOA(z.Name, z, record)
		} else {
			fqdnKey := dns.Fqdn(key) + z.Name
			var as []dns.RR
			var xs []dns.RR

			location := redis.findLocation(fqdnKey, z)
			record := redis.get(location, z)

			// Pull all zone records
			as, xs = redis.A(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.AAAA(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.CNAME(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.MX(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.SRV(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)

			as, xs = redis.TXT(fqdnKey, z, record)
			answers = append(answers, as...)
			extras = append(extras, xs...)
		}
	}

	records = soa
	records = append(records, answers...)
	records = append(records, extras...)
	records = append(records, soa...)

	log.Debugf("built AXFR response for zone %s with %d records", z.Name, len(records))
	return
}

func (redis *Redis) hosts(name string, z *Zone) []dns.RR {
	var (
		record  *Record
		answers []dns.RR
	)
	location := redis.findLocation(name, z)
	if location == "" {
		log.Debugf("no additional records found for host %s in zone %s", name, z.Name)
		return nil
	}
	log.Debugf("loading additional records for host %s from location %s in zone %s", name, location, z.Name)
	record = redis.get(location, z)
	a, _ := redis.A(name, z, record)
	answers = append(answers, a...)
	aaaa, _ := redis.AAAA(name, z, record)
	answers = append(answers, aaaa...)
	cname, _ := redis.CNAME(name, z, record)
	answers = append(answers, cname...)
	return answers
}

func (redis *Redis) serial() uint32 {
	return uint32(time.Now().Unix())
}

func (redis *Redis) minTtl(ttl uint32) uint32 {
	if redis.Ttl == 0 && ttl == 0 {
		return defaultTtl
	}
	if redis.Ttl == 0 {
		return ttl
	}
	if ttl == 0 {
		return redis.Ttl
	}
	if redis.Ttl < ttl {
		return redis.Ttl
	}
	return ttl
}

func (redis *Redis) findLocation(query string, z *Zone) string {
	var (
		ok                                 bool
		closestEncloser, sourceOfSynthesis string
	)

	// request for zone records
	if query == z.Name {
		log.Debugf("query %s matched zone apex %s", query, z.Name)
		return query
	}

	query = strings.TrimSuffix(query, "."+z.Name)

	if _, ok = z.Locations[query]; ok {
		log.Debugf("query matched exact redis location %s in zone %s", query, z.Name)
		return query
	}

	closestEncloser, sourceOfSynthesis, ok = splitQuery(query)
	for ok {
		ceExists := keyMatches(closestEncloser, z) || keyExists(closestEncloser, z)
		ssExists := keyExists(sourceOfSynthesis, z)
		if ceExists {
			if ssExists {
				log.Debugf("query %s matched wildcard redis location %s in zone %s", query, sourceOfSynthesis, z.Name)
				return sourceOfSynthesis
			} else {
				log.Debugf("query %s has closest encloser %s but no wildcard source in zone %s", query, closestEncloser, z.Name)
				return ""
			}
		} else {
			closestEncloser, sourceOfSynthesis, ok = splitQuery(closestEncloser)
		}
	}
	log.Debugf("query %s did not match any redis location in zone %s", query, z.Name)
	return ""
}

func (redis *Redis) get(key string, z *Zone) *Record {
	var (
		err   error
		reply interface{}
		val   string
	)
	conn := redis.Pool.Get()
	if conn == nil {
		log.Error("error connecting to redis")
		return nil
	}
	defer conn.Close()

	var label string
	if key == z.Name {
		label = "@"
	} else {
		label = key
	}

	redisKey := redis.keyPrefix + z.Name + redis.keySuffix
	log.Debugf("loading redis record: key=%q field=%q", redisKey, label)
	reply, err = conn.Do("HGET", redisKey, label)
	if err != nil {
		log.Errorf("error getting redis record key %q field %q: %v", redisKey, label, err)
		return nil
	}
	val, err = redisCon.String(reply, nil)
	if err != nil {
		log.Errorf("error parsing redis record key %q field %q as string: %v", redisKey, label, err)
		return nil
	}
	r := new(Record)
	err = json.Unmarshal([]byte(val), r)
	if err != nil {
		log.Errorf("JSON-decoding error for redis key %q field %q: %v", redisKey, label, err)
		return nil
	}
	log.Debugf("loaded redis record: key=%q field=%q", redisKey, label)
	return r
}

func keyExists(key string, z *Zone) bool {
	_, ok := z.Locations[key]
	return ok
}

func keyMatches(key string, z *Zone) bool {
	for value := range z.Locations {
		if strings.HasSuffix(value, key) {
			return true
		}
	}
	return false
}

func splitQuery(query string) (string, string, bool) {
	if query == "" {
		return "", "", false
	}
	var (
		splits            []string
		closestEncloser   string
		sourceOfSynthesis string
	)
	splits = strings.SplitAfterN(query, ".", 2)
	if len(splits) == 2 {
		closestEncloser = splits[1]
		sourceOfSynthesis = "*." + closestEncloser
	} else {
		closestEncloser = ""
		sourceOfSynthesis = "*"
	}
	return closestEncloser, sourceOfSynthesis, true
}

func (redis *Redis) Connect() {
	network, address := redis.dialAddress()
	log.Debugf("configuring redis connection pool: network=%s address=%q", network, address)
	redis.Pool = &redisCon.Pool{
		Dial: func() (redisCon.Conn, error) {
			opts := []redisCon.DialOption{}
			if redis.redisPassword != "" {
				opts = append(opts, redisCon.DialPassword(redis.redisPassword))
			}
			if redis.connectTimeout != 0 {
				opts = append(opts, redisCon.DialConnectTimeout(time.Duration(redis.connectTimeout)*time.Millisecond))
			}
			if redis.readTimeout != 0 {
				opts = append(opts, redisCon.DialReadTimeout(time.Duration(redis.readTimeout)*time.Millisecond))
			}

			log.Debugf("dialing redis: network=%s address=%q", network, address)
			conn, err := redisCon.Dial(network, address, opts...)
			if err != nil {
				log.Errorf("error dialing redis network %s address %q: %v", network, address, err)
				return nil, err
			}
			log.Debugf("connected to redis: network=%s address=%q", network, address)
			return conn, nil
		},
	}
}

func (redis *Redis) dialAddress() (network, address string) {
	address = redis.redisAddress
	switch {
	case strings.HasPrefix(address, "unix://"):
		return "unix", strings.TrimPrefix(address, "unix://")
	case strings.HasPrefix(address, "unix:"):
		return "unix", strings.TrimPrefix(address, "unix:")
	case strings.HasPrefix(address, "/"):
		return "unix", address
	default:
		return "tcp", address
	}
}

func (redis *Redis) save(zone string, subdomain string, value string) error {
	var err error

	conn := redis.Pool.Get()
	if conn == nil {
		log.Error("error connecting to redis")
		return nil
	}
	defer conn.Close()

	redisKey := redis.keyPrefix + zone + redis.keySuffix
	log.Debugf("saving redis record: key=%q field=%q", redisKey, subdomain)
	_, err = conn.Do("HSET", redisKey, subdomain, value)
	if err != nil {
		log.Errorf("error saving redis record key %q field %q: %v", redisKey, subdomain, err)
		return err
	}
	log.Debugf("saved redis record: key=%q field=%q", redisKey, subdomain)
	return err
}

func (redis *Redis) load(zone string) *Zone {
	var (
		reply interface{}
		err   error
		vals  []string
	)

	conn := redis.Pool.Get()
	if conn == nil {
		log.Error("error connecting to redis")
		return nil
	}
	defer conn.Close()

	redisKey := redis.keyPrefix + zone + redis.keySuffix
	log.Debugf("loading redis zone: key=%q", redisKey)
	reply, err = conn.Do("HKEYS", redisKey)
	if err != nil {
		log.Errorf("error loading redis zone key %q: %v", redisKey, err)
		return nil
	}
	z := new(Zone)
	z.Name = zone
	vals, err = redisCon.Strings(reply, nil)
	if err != nil {
		log.Errorf("error parsing redis zone key %q fields: %v", redisKey, err)
		return nil
	}
	z.Locations = make(map[string]struct{})
	for _, val := range vals {
		z.Locations[val] = struct{}{}
	}

	log.Debugf("loaded redis zone %s with %d locations", zone, len(z.Locations))
	return z
}

// decodeScanReply parses redigo's response from the SCAN command and returns it as
// structured data.
func decodeScanReply(reply interface{}) (scanReply *RedisScanReply, err error) {
	scanReply = &RedisScanReply{}

	// Redigo encodes the reply as an interface{}, so here we are converting it to something
	// useful.  Under the covers, the reply is [][]byte{}.

	// the cursor is []byte encoded in index 0
	cursorBytes := reply.([]interface{})[0].([]uint8)
	cursor, err := strconv.Atoi(string(cursorBytes))
	if err != nil {
		log.Errorf("error parsing redis SCAN cursor %q: %v", string(cursorBytes), err)
		return nil, err
	}
	scanReply.cursor = cursor

	// keys are []byte encoded starting with index 1
	for _, value := range reply.([]interface{})[1:] {

		redisKeys := value.([]interface{})
		for _, redisKey := range redisKeys {

			zone := string(redisKey.([]uint8))
			scanReply.keys = append(scanReply.keys, zone)
		}
	}
	return scanReply, nil
}

func split255(s string) []string {
	if len(s) < 255 {
		return []string{s}
	}
	sx := []string{}
	p, i := 0, 255
	for {
		if i <= len(s) {
			sx = append(sx, s[p:i])
		} else {
			sx = append(sx, s[p:])
			break

		}
		p, i = p+255, i+255
	}

	return sx
}

const (
	defaultTtl     = 360
	hostmaster     = "hostmaster"
	zoneUpdateTime = 10 * time.Minute
	transferLength = 1000
)
