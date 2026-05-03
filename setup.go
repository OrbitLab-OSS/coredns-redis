package redis

import (
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/coredns/caddy"
	"github.com/coredns/coredns/core/dnsserver"
	"github.com/coredns/coredns/plugin"
)

func init() {
	caddy.RegisterPlugin("redis", caddy.Plugin{
		ServerType: "dns",
		Action:     setup,
	})
}

func setup(c *caddy.Controller) error {
	instance, err := redisParse(c)
	if err != nil {
		return plugin.Error("redis", err)
	}

	dnsserver.GetConfig(c).AddPlugin(func(next plugin.Handler) plugin.Handler {
		instance.Next = next
		return instance
	})

	return nil
}

func redisParse(c *caddy.Controller) (*Redis, error) {
	cfg := pluginConfig{
		defaultTTL:    defaultPluginTTL,
		zoneRefresh:   defaultZoneRefresh,
		maxRecordSize: defaultRecordSizeLimit,
		backend: backendConfig{
			address:             defaultRedisAddress,
			scanCount:           defaultScanCount,
			maxRecordSize:       defaultRecordSizeLimit,
			maxIdle:             defaultRedisMaxIdle,
			maxActive:           defaultRedisMaxActive,
			idleTimeout:         defaultRedisIdleTimeout,
			wait:                true,
			borrowCheckInterval: defaultBorrowCheck,
		},
		cache: cacheConfig{
			path:            filepath.Join(os.TempDir(), "coredns-redis-cache"),
			maxEntries:      defaultCacheMaxEntries,
			maxEntrySize:    defaultCacheMaxEntrySize,
			cleanupInterval: defaultCacheCleanup,
		},
	}

	for c.Next() {
		if !c.NextBlock() {
			continue
		}

		for {
			switch c.Val() {
			case "address":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.address = value
			case "password":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.password = value
			case "database":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.database = uint32(parseIntOrDefault(value, defaultDatabase))
			case "prefix":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.keyPrefix = value
			case "suffix":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.keySuffix = value
			case "connect_timeout":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.connectTimeout = time.Duration(parseIntOrDefault(value, 0)) * time.Millisecond
			case "read_timeout":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.readTimeout = time.Duration(parseIntOrDefault(value, 0)) * time.Millisecond
			case "ttl":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.defaultTTL = uint32(parseIntOrDefault(value, defaultPluginTTL))
			case "fallthrough":
				cfg.fall.SetZonesFromArgs(c.RemainingArgs())
			case "cache_path":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.cache.path = value
			case "cache_cleanup_interval":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.cache.cleanupInterval, err = time.ParseDuration(value)
				if err != nil {
					return nil, c.Errf("invalid cache_cleanup_interval %q: %v", value, err)
				}
			case "cache_max_entries":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.cache.maxEntries = parseIntOrDefault(value, defaultCacheMaxEntries)
			case "cache_max_entry_size":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.cache.maxEntrySize = parseIntOrDefault(value, defaultCacheMaxEntrySize)
			case "zone_refresh":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.zoneRefresh, err = time.ParseDuration(value)
				if err != nil {
					return nil, c.Errf("invalid zone_refresh %q: %v", value, err)
				}
			case "scan_count":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.scanCount = parseIntOrDefault(value, defaultScanCount)
			case "max_record_size":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.maxRecordSize = parseIntOrDefault(value, defaultRecordSizeLimit)
				cfg.backend.maxRecordSize = cfg.maxRecordSize
			case "redis_max_idle":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.maxIdle = parseIntOrDefault(value, defaultRedisMaxIdle)
			case "redis_max_active":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.maxActive = parseIntOrDefault(value, defaultRedisMaxActive)
			case "redis_idle_timeout":
				value, err := nextArg(c)
				if err != nil {
					return nil, err
				}
				cfg.backend.idleTimeout, err = time.ParseDuration(value)
				if err != nil {
					return nil, c.Errf("invalid redis_idle_timeout %q: %v", value, err)
				}
			default:
				if c.Val() != "}" {
					return nil, c.Errf("unknown property %q", c.Val())
				}
			}

			if !c.Next() {
				break
			}
		}
	}

	if cfg.zoneRefresh <= 0 {
		return nil, c.Err("zone_refresh must be greater than zero")
	}
	if cfg.cache.cleanupInterval <= 0 {
		return nil, c.Err("cache_cleanup_interval must be greater than zero")
	}
	if cfg.cache.maxEntries <= 0 {
		return nil, c.Err("cache_max_entries must be greater than zero")
	}
	if cfg.cache.maxEntrySize <= 0 {
		return nil, c.Err("cache_max_entry_size must be greater than zero")
	}
	if cfg.maxRecordSize <= 0 {
		return nil, c.Err("max_record_size must be greater than zero")
	}
	cfg.backend.maxRecordSize = cfg.maxRecordSize

	return newRedisPlugin(cfg)
}

func nextArg(c *caddy.Controller) (string, error) {
	if !c.NextArg() {
		return "", c.ArgErr()
	}
	return c.Val(), nil
}

func parseIntOrDefault(value string, fallback int) int {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
