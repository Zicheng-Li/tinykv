package engine

import "container/list"

type cacheValue struct {
	key   string
	value []byte
}

type lruCache struct {
	capacity int
	items    map[string]*list.Element
	order    *list.List
}

func newLRU(capacity int) *lruCache {
	if capacity <= 0 {
		return nil
	}
	return &lruCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

func (c *lruCache) get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(elem)
	val := elem.Value.(cacheValue).value
	return cloneBytes(val), true
}

func (c *lruCache) put(key string, value []byte) {
	if c == nil {
		return
	}
	if elem, ok := c.items[key]; ok {
		elem.Value = cacheValue{key: key, value: cloneBytes(value)}
		c.order.MoveToFront(elem)
		return
	}
	elem := c.order.PushFront(cacheValue{key: key, value: cloneBytes(value)})
	c.items[key] = elem
	if c.order.Len() <= c.capacity {
		return
	}
	last := c.order.Back()
	if last == nil {
		return
	}
	c.order.Remove(last)
	delete(c.items, last.Value.(cacheValue).key)
}

func (c *lruCache) remove(key string) {
	if c == nil {
		return
	}
	elem, ok := c.items[key]
	if !ok {
		return
	}
	c.order.Remove(elem)
	delete(c.items, key)
}

