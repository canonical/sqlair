package reflect

import (
	"reflect"
	"sync"
)

var (
	singleCache *cache
	once        sync.Once
)

// Cache enforces the singleton pattern,
// ensuring access to a single instance of cache.
func Cache() *cache {
	once.Do(func() {
		singleCache = &cache{
			cache: make(map[reflect.Type]Info),
		}
	})

	return singleCache
}
