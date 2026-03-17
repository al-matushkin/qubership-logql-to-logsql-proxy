package vlogs

import (
	"sync"
	"time"
)

// MetadataCache is a bounded, TTL-based in-memory cache for VictoriaLogs field
// name and field value lists. It stores only string slices (label names and
// their distinct values) — never log records — so its memory footprint is
// negligible (kilobytes) even with hundreds of entries.
//
// Thread-safe: reads use a shared lock, writes use an exclusive lock.
type MetadataCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
	maxSize int
}

type cacheEntry struct {
	value     []string
	expiresAt time.Time
}

// NewMetadataCache creates a MetadataCache that holds at most maxSize entries.
// Beyond that limit the oldest (arbitrary) entry is evicted to make room.
// Pass maxSize ≤ 0 to use a default of 256.
func NewMetadataCache(maxSize int) *MetadataCache {
	if maxSize <= 0 {
		maxSize = 256
	}
	return &MetadataCache{
		entries: make(map[string]cacheEntry, maxSize),
		maxSize: maxSize,
	}
}

// Get returns the cached string slice for key if it exists and has not expired.
// The second return value is false on a miss or expiry.
func (c *MetadataCache) Get(key string) ([]string, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.value, true
}

// Set stores value under key with the given TTL. If the cache is at capacity,
// expired entries are evicted first; if still full, one arbitrary live entry is
// removed to make room.
func (c *MetadataCache) Set(key string, value []string, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.maxSize {
		c.evictExpiredLocked()
		if len(c.entries) >= c.maxSize {
			// Still full after expiry sweep: drop one arbitrary entry.
			for k := range c.entries {
				delete(c.entries, k)
				break
			}
		}
	}

	c.entries[key] = cacheEntry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
}

// evictExpiredLocked removes all entries whose TTL has elapsed.
// Must be called with c.mu held for writing.
func (c *MetadataCache) evictExpiredLocked() {
	now := time.Now()
	for k, e := range c.entries {
		if now.After(e.expiresAt) {
			delete(c.entries, k)
		}
	}
}

// FieldNamesKey returns the cache key used for /field_names responses.
// The start and end times are rounded to the nearest minute to maximise
// hit rate across slightly different Grafana requests.
func FieldNamesKey(start, end time.Time) string {
	return "names:" + roundMinute(start) + ":" + roundMinute(end)
}

// FieldValuesKey returns the cache key used for /field_values responses.
func FieldValuesKey(field string, start, end time.Time) string {
	return "values:" + field + ":" + roundMinute(start) + ":" + roundMinute(end)
}

// roundMinute truncates t to the nearest minute and formats it as a compact
// decimal Unix timestamp string, used as a stable cache key component.
func roundMinute(t time.Time) string {
	rounded := t.UTC().Truncate(time.Minute)
	return rounded.Format("20060102T1504Z")
}
