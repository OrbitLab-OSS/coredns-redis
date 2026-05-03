package redis

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const cacheFormatVersion = 1

type diskCache struct {
	root            string
	maxEntries      int
	maxEntrySize    int
	cleanupInterval time.Duration

	writeMu     sync.Mutex
	cleanupMu   sync.Mutex
	lastCleanup time.Time
}

type cacheConfig struct {
	path            string
	maxEntries      int
	maxEntrySize    int
	cleanupInterval time.Duration
}

type cacheEntry struct {
	Version   int    `json:"version"`
	Zone      string `json:"zone"`
	Label     string `json:"label"`
	Payload   string `json:"payload"`
	ExpiresAt int64  `json:"expires_at_unix"`
	StoredAt  int64  `json:"stored_at_unix"`
}

type cacheFileInfo struct {
	path    string
	modTime time.Time
}

func newDiskCache(cfg cacheConfig) (*diskCache, error) {
	if cfg.path == "" {
		return nil, errors.New("cache path is required")
	}
	if cfg.maxEntries <= 0 {
		return nil, errors.New("cache max entries must be greater than zero")
	}
	if cfg.maxEntrySize <= 0 {
		return nil, errors.New("cache max entry size must be greater than zero")
	}

	root := filepath.Join(filepath.Clean(cfg.path), "v1")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}

	return &diskCache{
		root:            root,
		maxEntries:      cfg.maxEntries,
		maxEntrySize:    cfg.maxEntrySize,
		cleanupInterval: cfg.cleanupInterval,
		lastCleanup:     time.Now(),
	}, nil
}

func (c *diskCache) Get(zone, label string, now time.Time) (string, bool, error) {
	filename := c.entryPath(zone, label)
	info, err := os.Stat(filename)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if info.Size() > int64(c.maxEntrySize) {
		_ = os.Remove(filename)
		return "", false, errCorruptCache
	}

	data, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		return "", false, err
	}
	if len(data) > c.maxEntrySize {
		_ = os.Remove(filename)
		return "", false, errCorruptCache
	}

	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		_ = os.Remove(filename)
		return "", false, errCorruptCache
	}
	if entry.Version != cacheFormatVersion || entry.Zone != normalizeZone(zone) || entry.Label != label || entry.Payload == "" {
		_ = os.Remove(filename)
		return "", false, errCorruptCache
	}
	if time.Unix(entry.ExpiresAt, 0).Before(now) {
		_ = os.Remove(filename)
		return "", false, nil
	}

	return entry.Payload, true, nil
}

func (c *diskCache) Put(zone, label, payload string, ttl time.Duration, now time.Time) error {
	if ttl <= 0 {
		return nil
	}
	if len(payload) == 0 || len(payload) > c.maxEntrySize {
		return nil
	}

	entry := cacheEntry{
		Version:   cacheFormatVersion,
		Zone:      normalizeZone(zone),
		Label:     label,
		Payload:   payload,
		ExpiresAt: now.Add(ttl).Unix(),
		StoredAt:  now.Unix(),
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	if len(data) > c.maxEntrySize {
		return nil
	}

	filename := c.entryPath(zone, label)
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	tmpFile, err := os.CreateTemp(filepath.Dir(filename), filepath.Base(filename)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filename); err != nil {
		return err
	}

	c.maybeCleanup(now)
	return nil
}

func (c *diskCache) maybeCleanup(now time.Time) {
	if c.cleanupInterval <= 0 {
		return
	}

	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()

	if now.Sub(c.lastCleanup) < c.cleanupInterval {
		return
	}
	c.lastCleanup = now
	_ = c.cleanup(now)
}

func (c *diskCache) cleanup(now time.Time) error {
	files := make([]cacheFileInfo, 0, 64)

	err := filepath.WalkDir(c.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}

		info, statErr := d.Info()
		if statErr != nil {
			_ = os.Remove(path)
			return nil
		}

		payload, hit, readErr := c.GetFromFile(path, now)
		if readErr != nil {
			_ = os.Remove(path)
			return nil
		}
		if !hit || payload == "" {
			return nil
		}

		files = append(files, cacheFileInfo{path: path, modTime: info.ModTime()})
		return nil
	})
	if err != nil {
		return err
	}

	if len(files) <= c.maxEntries {
		return nil
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.Before(files[j].modTime)
	})

	for _, file := range files[:len(files)-c.maxEntries] {
		_ = os.Remove(file.path)
	}

	return nil
}

func (c *diskCache) GetFromFile(path string, now time.Time) (string, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", false, err
	}
	if info.Size() > int64(c.maxEntrySize) {
		return "", false, errCorruptCache
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return "", false, err
	}
	var entry cacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return "", false, errCorruptCache
	}
	if entry.Version != cacheFormatVersion || entry.Payload == "" {
		return "", false, errCorruptCache
	}
	if time.Unix(entry.ExpiresAt, 0).Before(now) {
		_ = os.Remove(path)
		return "", false, nil
	}
	return entry.Payload, true, nil
}

func (c *diskCache) entryPath(zone, label string) string {
	key := sha256.Sum256([]byte(normalizeZone(zone) + "\x00" + label))
	encoded := hex.EncodeToString(key[:])
	return filepath.Join(c.root, encoded[:2], encoded+".json")
}
