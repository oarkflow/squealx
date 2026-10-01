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
	if len(namedCache.m) >= namedCacheMaxEntries {
		namedCache.m = make(map[namedCacheKey]namedCacheVal, 64)
	}
	namedCache.m[key] = namedCacheVal{bound, names}
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
