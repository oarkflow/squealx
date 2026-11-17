package squealx

import (
	"sync"
	"unicode"
	"unicode/utf8"
)

const (
	namedCacheMaxEntries  = 2048
	namedCacheMaxQueryLen = 8 << 10
)

type namedCacheKey struct {
	q string
	b int
}

type namedCacheVal struct {
	bound string
	names []string
}

// namedCache is a bounded cache of successful compileNamedQuery results.
// The cached names slice is shared between callers and must be treated as
// read-only.
var namedCache = struct {
	sync.RWMutex
	m map[namedCacheKey]namedCacheVal
	// FIFO eviction avoids clearing every hot entry whenever the cache fills.
	// The fixed ring adds no per-insertion allocation.
	order [namedCacheMaxEntries]namedCacheKey
	next  int
}{m: make(map[namedCacheKey]namedCacheVal, 64)}

// compileNamedQueryCached is compileNamedQuery with memoization. The returned
// names slice is shared and must not be modified.
func compileNamedQueryCached(query string, bindType int) (string, []string, error) {
	if len(query) > namedCacheMaxQueryLen {
		return compileNamedQuery(query, bindType)
	}
	key := namedCacheKey{query, bindType}
	namedCache.RLock()
	v, ok := namedCache.m[key]
	namedCache.RUnlock()
	if ok {
		return v.bound, v.names, nil
	}
	bound, names, err := compileNamedQuery(query, bindType)
	if err != nil {
		return bound, names, err
	}
	namedCache.Lock()
	// Another goroutine may have populated this key after our read miss.
	if _, exists := namedCache.m[key]; exists {
		namedCache.Unlock()
		return bound, names, nil
	}
	if len(namedCache.m) >= namedCacheMaxEntries {
		oldest := namedCache.order[namedCache.next]
		delete(namedCache.m, oldest)
	}
	namedCache.m[key] = namedCacheVal{bound, names}
	namedCache.order[namedCache.next] = key
	namedCache.next = (namedCache.next + 1) % namedCacheMaxEntries
	namedCache.Unlock()
	return bound, names, nil
}

// mayContainNamedParam is an allocation-free pre-check: it reports whether the
// query has a ':' followed by a letter or underscore.
func mayContainNamedParam(q string) bool {
	for i := 0; i+1 < len(q); i++ {
		if q[i] != ':' {
			continue
		}
		c := q[i+1]
		if c == '_' || (c|0x20 >= 'a' && c|0x20 <= 'z') {
			return true
		}
		if c >= utf8.RuneSelf {
			if r, _ := utf8.DecodeRuneInString(q[i+1:]); unicode.IsLetter(r) {
				return true
			}
		}
	}
	return false
}

// inMatches reports whether query matches InReg, guarded by the cheap
// mayContainIN prefilter so the regexp engine only runs when a standalone
// "in" token is actually present.
func inMatches(query string) bool {
	return mayContainIN(query) && InReg.MatchString(query)
}

// isWordByte reports whether b can be part of an identifier, for the purposes
// of the word-boundary check in mayContainIN.
func isWordByte(b byte) bool {
	return b == '_' || (b|0x20 >= 'a' && b|0x20 <= 'z') || (b >= '0' && b <= '9')
}

// mayContainIN is an allocation-free pre-check: it reports whether the query
// contains a standalone "in" token (case-insensitive), which is a necessary
// condition for InReg to match. This lets callers skip running InReg's regexp
// engine over the full query text for the common case of queries with no IN
// clause at all.
func mayContainIN(q string) bool {
	for i := 0; i+1 < len(q); i++ {
		if q[i]|0x20 != 'i' || q[i+1]|0x20 != 'n' {
			continue
		}
		if i > 0 && isWordByte(q[i-1]) {
			continue
		}
		if i+2 < len(q) && isWordByte(q[i+2]) {
			continue
		}
		return true
	}
	return false
}
