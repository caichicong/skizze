/*
This file is a modified version of Google's groupcache LRU which is licensed
under http://www.apache.org/licenses/LICENSE-2.0
*/

package smartfile

import "container/list"

// Cache is an LRU cache. It is not safe for concurrent access.
type Cache struct {
	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specificies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key int64, value *item)

	ll    *list.List
	cache map[int64]*list.Element
}

type entry struct {
	key   int64
	value *item
}

// NewLRU creates a new Cache.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func NewLRU(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[int64]*list.Element),
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key int64, value item) {
	if c.cache == nil {
		c.cache = make(map[int64]*list.Element)
		c.ll = list.New()
	}
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = &value
		return
	}
	ele := c.ll.PushFront(&entry{key, &value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.removeOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *Cache) get(key int64) (value *item, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *Cache) remove(key int64) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) removeOldest() {
	if c.cache == nil {
		return
	}
	ele := c.ll.Back()
	oldest := ele
	for {
		if ele == nil {
			break
		}
		ele = ele.Prev()
		if ele != nil && !ele.Value.(*entry).value.dirty {
			c.removeElement(ele)
			return
		}
	}
	if oldest != nil {
		c.removeElement(oldest)
		return
	}
}

func (c *Cache) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (c *Cache) len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Clear removes all items from the cache.
func (c *Cache) clear() {
	if c.OnEvicted != nil {
		for k, v := range c.cache {
			c.OnEvicted(k, v.Value.(*entry).value)
		}
	}

	c.ll = list.New()
	c.cache = make(map[int64]*list.Element)
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *Cache) keys() []int64 {
	keys := make([]int64, len(c.cache))
	ele := c.ll.Back()
	i := 0
	for ele != nil {
		keys[i] = ele.Value.(*entry).key
		ele = ele.Prev()
		i++
	}
	return keys
}

// Peek returns the key's value (or nil if not found) without updating the cache.
func (c *Cache) peek(key int64) (value *item, ok bool) {
	if ele, ok := c.cache[key]; ok {
		return ele.Value.(*entry).value, true
	}
	return nil, ok
}
