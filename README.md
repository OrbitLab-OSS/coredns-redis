# CoreDNS Redis Plugin

`redis` is a CoreDNS authoritative plugin backed by Redis with a local on-disk pull-through cache.

OrbitLab uses this plugin as the primary DNS path for VM and LXC workloads, so this implementation is intentionally opinionated toward predictable production behavior:

- Redis is read-only from the plugin.
- Zone discovery and zone field refreshes are bounded by refresh intervals instead of per-request invalidation tricks.
- Record payloads are cached on disk with TTL-bounded expiration.
- Valid cache entries are served during Redis outages.
- Expired cache entries are never served.
- Malformed Redis or cache payloads are ignored and refreshed when possible.

## Capabilities

- Authoritative responses for `A`, `AAAA`, `CNAME`, `TXT`, `NS`, `MX`, `SRV`, `SOA`, and `CAA`
- AXFR support
- Exact and wildcard lookups
- Relative target normalization inside a zone
- Additional section generation for `NS`, `MX`, and `SRV`
- Persistent on-disk record cache
- Read-only Redis backend

## Redis Access Model

The plugin never writes to Redis.

It only uses read operations:

- `SCAN` for zone discovery
- `HKEYS` for per-zone location refresh
- `HGET` for record reads
- `PING` for pooled connection health checks

There are no `HSET`, `SET`, `DEL`, or other mutation paths in the plugin.

## Cache Model

The cache is a durable file-per-entry store on local disk.

- Cache key: `zone + label`
- Cache value: the raw Redis JSON payload plus metadata
- Expiration: derived from the lowest effective TTL in the record payload
- Writes: atomic temp-file + rename
- Corruption handling: bad cache files are ignored and removed on read
- Cleanup: periodic removal of expired entries plus oldest-entry trimming when the cache exceeds `cache_max_entries`

If Redis is unavailable:

- a valid cache entry is served
- an expired cache entry is not served
- a missing cache entry results in authoritative failure behavior

Negative caching is intentionally not enabled.

## Corefile Syntax

```corefile
redis {
    address localhost:6379
    password foobared
    database 0
    prefix _dns:
    suffix :zone

    connect_timeout 100
    read_timeout 100
    ttl 300

    cache_path /var/lib/coredns/redis-cache
    cache_cleanup_interval 10m
    cache_max_entries 10000
    cache_max_entry_size 1048576

    zone_refresh 1m
    scan_count 1000
    max_record_size 1048576

    redis_max_idle 8
    redis_max_active 64
    redis_idle_timeout 5m

    fallthrough example.internal.
}
```

## Configuration

### Redis settings

- `address`: Redis address in `host:port` form or Unix socket path. Supported Unix formats:
  - `/var/run/redis/redis.sock`
  - `unix:/var/run/redis/redis.sock`
  - `unix:///var/run/redis/redis.sock`
- `password`: Redis AUTH password
- `database`: Redis database number
- `prefix`: Prefix applied to zone hash keys
- `suffix`: Suffix applied to zone hash keys
- `connect_timeout`: Redis dial timeout in milliseconds
- `read_timeout`: Redis read timeout in milliseconds
- `redis_max_idle`: Max idle pooled Redis connections
- `redis_max_active`: Max active pooled Redis connections
- `redis_idle_timeout`: Idle connection timeout, parsed with Go duration syntax
- `scan_count`: Redis `SCAN COUNT` hint used for zone discovery
- `max_record_size`: Hard limit in bytes for a Redis record payload

### DNS and refresh settings

- `ttl`: Default DNS TTL used when a record TTL is missing or zero
- `zone_refresh`: How often zone names and per-zone field lists may be refreshed from Redis
- `fallthrough`: Pass `NXDOMAIN` responses to the next plugin. If zones are listed, only those zones fall through

### Cache settings

- `cache_path`: Root directory for the persistent on-disk cache
- `cache_cleanup_interval`: How often expired and excess entries are cleaned up
- `cache_max_entries`: Maximum number of cache files retained after cleanup
- `cache_max_entry_size`: Maximum size in bytes for an individual cache file

If `cache_path` is not explicitly set, the plugin defaults to `${TMPDIR}/coredns-redis-cache`. For production, set this to a persistent directory such as `/var/lib/coredns/redis-cache`.

## Operational Behavior

### Zone refresh

- Zone names are discovered from Redis with `SCAN`
- A matched zone is refreshed from Redis with `HKEYS` at most once per `zone_refresh`
- If a zone refresh fails, the plugin keeps serving the last successfully loaded zone index

### Record lookup

1. Normalize the DNS question name
2. Match the zone from the in-memory zone list
3. Resolve the zone-relative label, including wildcard handling
4. Check the on-disk cache for a valid unexpired entry
5. If cache miss or expired, fetch from Redis with `HGET`
6. Decode, validate, cache, and serve the record

### TTL behavior

- Each RR still carries its own TTL in the DNS answer
- Cache expiration uses the lowest effective TTL found in the cached record payload
- A missing or zero TTL falls back to the configured plugin default
- Cache entries are never allowed to outlive their effective TTL

## Failure Behavior

### Redis outage with valid cache

The plugin serves the cached record.

### Redis outage without valid cache

The plugin returns an authoritative failure response for records that cannot be satisfied.

### Corrupt cache entry

The cache file is ignored and removed, and the plugin retries against Redis.

### Malformed Redis JSON

The plugin does not cache the payload and returns authoritative failure behavior for that lookup.

## Redis Schema

Each zone is stored as a Redis hash. The hash key is the fully qualified zone name with any configured prefix or suffix applied.

Example keys:

```text
example.com.
example.net.
```

Each hash field is a zone-relative label:

- `@` for the zone apex
- `host1`
- `_sip._tcp`
- `*`

Each field value is a JSON document containing one or more RR sets.

Example:

```json
{
  "a": [
    { "ttl": 300, "ip": "1.2.3.4" }
  ],
  "mx": [
    { "ttl": 300, "host": "mail.example.com.", "preference": 10 }
  ]
}
```

Relative host targets are allowed for `NS`, `MX`, `SRV`, `CNAME`, and `SOA` names. They are normalized against the zone before being served.

## Build And Test

```bash
go test ./...
go test -race ./...
```

## Production Notes

- Use a persistent `cache_path`
- Size `redis_max_active` for your CoreDNS concurrency
- Keep `zone_refresh` short enough for acceptable control-plane propagation, but not so short that Redis is hammered
- Keep `max_record_size` conservative to reject abusive payloads
- Put this plugin near other authoritative backends in `plugin.cfg`
