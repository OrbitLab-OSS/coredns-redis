package redis

import (
	"errors"
	"sort"
	"strings"
	"time"

	redisCon "github.com/gomodule/redigo/redis"
)

var errRecordNotFound = errors.New("redis record not found")

type recordBackend interface {
	ListZones() ([]string, error)
	LoadZone(zone string) (*Zone, error)
	GetRecord(zone, label string) (string, error)
}

type redisBackend struct {
	pool          *redisCon.Pool
	keyPrefix     string
	keySuffix     string
	scanCount     int
	maxRecordSize int
}

func newRedisBackend(cfg backendConfig) *redisBackend {
	network, address := dialAddress(cfg.address)

	return &redisBackend{
		keyPrefix:     cfg.keyPrefix,
		keySuffix:     cfg.keySuffix,
		scanCount:     cfg.scanCount,
		maxRecordSize: cfg.maxRecordSize,
		pool: &redisCon.Pool{
			MaxIdle:     cfg.maxIdle,
			MaxActive:   cfg.maxActive,
			IdleTimeout: cfg.idleTimeout,
			Wait:        cfg.wait,
			TestOnBorrow: func(conn redisCon.Conn, lastUsed time.Time) error {
				if time.Since(lastUsed) < cfg.borrowCheckInterval {
					return nil
				}
				_, err := conn.Do("PING")
				return err
			},
			Dial: func() (redisCon.Conn, error) {
				return redisCon.Dial(network, address, buildDialOptions(cfg)...)
			},
		},
	}
}

func (b *redisBackend) ListZones() ([]string, error) {
	conn, err := b.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cursor := 0
	seen := make(map[string]struct{})
	zones := make([]string, 0, 16)

	for {
		reply, err := conn.Do("SCAN", cursor, "MATCH", b.keyPrefix+"*"+b.keySuffix, "COUNT", b.scanCount)
		if err != nil {
			return nil, err
		}

		scanReply, err := decodeScanReply(reply)
		if err != nil {
			return nil, err
		}
		cursor = scanReply.cursor

		for _, redisKey := range scanReply.keys {
			zone := strings.TrimPrefix(redisKey, b.keyPrefix)
			zone = strings.TrimSuffix(zone, b.keySuffix)
			zone = normalizeZone(zone)
			if zone == "" || !isDomainName(zone) {
				continue
			}
			if _, ok := seen[zone]; ok {
				continue
			}
			seen[zone] = struct{}{}
			zones = append(zones, zone)
		}

		if cursor == 0 {
			break
		}
	}

	sort.Strings(zones)
	return zones, nil
}

func (b *redisBackend) LoadZone(zone string) (*Zone, error) {
	conn, err := b.getConn()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	reply, err := conn.Do("HKEYS", b.redisKey(zone))
	if err != nil {
		return nil, err
	}

	fields, err := redisCon.Strings(reply, nil)
	if err != nil {
		return nil, err
	}

	locations := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if !isValidLocation(field, zone) {
			continue
		}
		locations[field] = struct{}{}
	}

	return &Zone{
		Name:        zone,
		Locations:   locations,
		RefreshedAt: time.Now(),
	}, nil
}

func (b *redisBackend) GetRecord(zone, label string) (string, error) {
	conn, err := b.getConn()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	reply, err := conn.Do("HGET", b.redisKey(zone), label)
	if err != nil {
		return "", err
	}

	value, err := redisCon.String(reply, nil)
	if errors.Is(err, redisCon.ErrNil) {
		return "", errRecordNotFound
	}
	if err != nil {
		return "", err
	}
	if len(value) > b.maxRecordSize {
		return "", errRecordTooLarge
	}
	return value, nil
}

func (b *redisBackend) getConn() (redisCon.Conn, error) {
	if b.pool == nil {
		return nil, errors.New("redis pool is not configured")
	}
	conn := b.pool.Get()
	if conn == nil {
		return nil, errors.New("redis connection is nil")
	}
	if err := conn.Err(); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (b *redisBackend) redisKey(zone string) string {
	return b.keyPrefix + normalizeZone(zone) + b.keySuffix
}

type backendConfig struct {
	address             string
	password            string
	database            uint32
	connectTimeout      time.Duration
	readTimeout         time.Duration
	writeTimeout        time.Duration
	keyPrefix           string
	keySuffix           string
	scanCount           int
	maxRecordSize       int
	maxIdle             int
	maxActive           int
	idleTimeout         time.Duration
	wait                bool
	borrowCheckInterval time.Duration
}

func buildDialOptions(cfg backendConfig) []redisCon.DialOption {
	options := make([]redisCon.DialOption, 0, 4)
	if cfg.password != "" {
		options = append(options, redisCon.DialPassword(cfg.password))
	}
	if cfg.database != 0 {
		options = append(options, redisCon.DialDatabase(int(cfg.database)))
	}
	if cfg.connectTimeout > 0 {
		options = append(options, redisCon.DialConnectTimeout(cfg.connectTimeout))
	}
	if cfg.readTimeout > 0 {
		options = append(options, redisCon.DialReadTimeout(cfg.readTimeout))
	}
	if cfg.writeTimeout > 0 {
		options = append(options, redisCon.DialWriteTimeout(cfg.writeTimeout))
	}
	return options
}

func dialAddress(address string) (network, resolved string) {
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
