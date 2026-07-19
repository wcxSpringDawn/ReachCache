/*
Copyright 2026 Wang Chunxiao (vernmorn)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// 本文件实现了标准 LRU（Least Recently Used）淘汰算法。
//
// 采用 container/list 双向链表 + map 哈希表的经典组合，实现 O(1) 时间复杂度的
// Get/Set 操作。Get 使用二段锁（RLock 查询 + Lock 更新链表位置）分离读写在锁粒度
// 上的竞争，提升并发吞吐量。
//
// 适合热点数据集中、访问具有强时间局部性的场景。不适用于存在周期性批量数据扫描的
// 场景——冷数据的批量访问会污染缓存，将热数据挤出（此时应使用 LRU-2）。
//
// 过期策略采用"惰性删除 + 定期清理"双机制：
//   - 惰性删除：Get 发现已过期时同步删除
//   - 定期清理：后台 goroutine 每分钟扫描 expiries 清理过期项与超容量条目

package store

import (
	"container/list"
	"sync"
	"time"
)

// lruCache 是标准 LRU 缓存的具体实现，基于 container/list 双向链表和 map 哈希表。
// 链表头部（Front）是最久未访问的数据，尾部（Back）是最近访问的数据。
// expires 从 items 独立出来，使得过期检查和 LRU 淘汰逻辑解耦。
type lruCache struct {
	mu              sync.RWMutex
	list            *list.List
	items           map[string]*list.Element // key → 链表节点，O(1) 定位
	expires         map[string]time.Time     // key → 过期时间，独立管理以解耦淘汰与过期
	maxBytes        int64
	usedBytes       int64
	onEvicted       func(key string, value Value) // 淘汰回调，在持锁外执行
	cleanupInterval time.Duration
	cleanupTicker   *time.Ticker
	closeCh         chan struct{}
}

// lruEntry 是链表节点 Element.Value 中存储的缓存条目。
type lruEntry struct {
	key   string
	value Value
}

// newLRUCache 创建一个新的 LRU 缓存实例并启动后台清理协程。
// 若 opts.CleanupInterval <= 0，则默认使用 1 分钟间隔。
func newLRUCache(opts Options) *lruCache {
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = time.Minute
	}

	c := &lruCache{
		list:            list.New(),
		items:           make(map[string]*list.Element),
		expires:         make(map[string]time.Time),
		maxBytes:        opts.MaxBytes,
		onEvicted:       opts.OnEvicted,
		cleanupInterval: cleanupInterval,
		closeCh:         make(chan struct{}),
	}

	c.cleanupTicker = time.NewTicker(c.cleanupInterval)
	go c.cleanupLoop()

	return c
}

// Get 获取缓存项。采用二段锁（Two-Phase Locking）优化：
//
//  1. 读锁阶段：O(1) 查找 items 映射并检查 expires 是否过期
//  2. 写锁阶段：将节点移至链表尾部表示最近访问
//
// 过期项立即同步删除
// 二段锁之间元素可能被其他协程删除，因此写锁阶段二次检查 items[key] 是否存在。
func (c *lruCache) Get(key string) (Value, bool) {
	c.mu.RLock()
	elem, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}

	// 检查是否过期
	if expTime, hasExp := c.expires[key]; hasExp && time.Now().After(expTime) {
		c.mu.RUnlock()

		// 删除过期项
		c.Delete(key)

		return nil, false
	}

	// 获取值并释放读锁
	entry := elem.Value.(*lruEntry)
	value := entry.value
	c.mu.RUnlock()

	// 读和写分成两段锁定，减少锁的持有时间，提高并发性能

	// 更新 LRU 位置需要写锁
	c.mu.Lock()
	// 再次检查元素是否仍然存在（可能在获取写锁期间被其他协程删除）
	if _, ok := c.items[key]; ok {
		c.list.MoveToBack(elem)
	}
	c.mu.Unlock()

	return value, true
}

// Set 添加或更新缓存项，永不过期。等价于 SetWithExpiration(key, value, 0)。
func (c *lruCache) Set(key string, value Value) error {
	// 默认设置永不过期
	return c.SetWithExpiration(key, value, 0)
}

// SetWithExpiration 添加或更新缓存项并设置存活时间（TTL）。
//
// 若 value 为 nil，等价于调用 Delete(key)。
// 若 expiration <= 0，缓存项永不过期（删除 expires 中的记录）。
// 每次写入后触发 evict 检查：先清理过期项，再按 LRU 顺序淘汰直到 usedBytes <= maxBytes。
func (c *lruCache) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	if value == nil {
		c.Delete(key)
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// 计算过期时间
	var expTime time.Time
	if expiration > 0 {
		expTime = time.Now().Add(expiration)
		c.expires[key] = expTime
	} else {
		delete(c.expires, key) // 当传入expiration=0时，表示不设置过期时间，删除任何现有的过期时间，缓存项变为永不过期
	}

	// 如果键已存在，更新值
	if elem, ok := c.items[key]; ok {
		oldEntry := elem.Value.(*lruEntry)
		c.usedBytes += int64(value.Len() - oldEntry.value.Len())
		oldEntry.value = value
		c.list.MoveToBack(elem)
		return nil
	}

	// 如果键不存在，添加新项
	entry := &lruEntry{key: key, value: value}
	elem := c.list.PushBack(entry)
	c.items[key] = elem
	c.usedBytes += int64(len(key) + value.Len())

	// 检查是否需要淘汰旧项
	c.evict()

	return nil
}

// removeElement 从链表、哈希表和过期映射中删除元素，更新 usedBytes，
// 并在 OnEvicted 已设置时触发回调。调用前必须持有写锁。
func (c *lruCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*lruEntry)
	c.list.Remove(elem)
	delete(c.items, entry.key)
	delete(c.expires, entry.key)
	c.usedBytes -= int64(len(entry.key) + entry.value.Len())

	if c.onEvicted != nil {
		c.onEvicted(entry.key, entry.value)
	}
}

// Delete 从缓存中删除指定键的项。返回 true 表示存在并已删除。
func (c *lruCache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		c.removeElement(elem)
		return true
	}
	return false
}

// evict 清理过期项和超出内存限制的项。先遍历 expires 删除过期条目，
// 再从链表头部（最久未使用）移除直到 usedBytes <= maxBytes。调用前必须持有锁。
func (c *lruCache) evict() {
	// 先清理过期项
	now := time.Now()
	for key, expTime := range c.expires {
		if now.After(expTime) {
			if elem, ok := c.items[key]; ok {
				c.removeElement(elem)
			}
		}
	}

	// 再根据内存限制清理最久未使用的项
	for c.maxBytes > 0 && c.usedBytes > c.maxBytes && c.list.Len() > 0 {
		elem := c.list.Front() // 获取最久未使用的项（链表头部）
		if elem != nil {
			c.removeElement(elem)
		}
	}
}

// Clear 清空缓存中的所有数据。若设置了 OnEvicted，会为每个条目触发回调。
// 清空后缓存仍可继续使用。
func (c *lruCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果设置了回调函数，遍历所有项调用回调
	if c.onEvicted != nil {
		for _, elem := range c.items {
			entry := elem.Value.(*lruEntry)
			c.onEvicted(entry.key, entry.value)
		}
	}

	c.list.Init()
	c.items = make(map[string]*list.Element)
	c.expires = make(map[string]time.Time)
	c.usedBytes = 0
}

// Len 返回当前缓存中的有效条目数量。
func (c *lruCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.list.Len()
}

// cleanupLoop 是后台定期清理协程，每隔 cleanupInterval 触发一次 evict。
func (c *lruCache) cleanupLoop() {
	for {
		select {
		case <-c.cleanupTicker.C:
			c.mu.Lock()
			c.evict()
			c.mu.Unlock()
		case <-c.closeCh:
			return
		}
	}
}

// Close 停止后台清理协程并释放定时器资源。
// 关闭后不应再使用该实例。
func (c *lruCache) Close() {
	if c.cleanupTicker != nil {
		c.cleanupTicker.Stop()
		close(c.closeCh)
	}
}

// GetWithExpiration 获取缓存项及其剩余过期时间（TTL）。
// 返回 (value, ttl, true) 表示命中；若已过期或不存在则返回 (nil, 0, false)。
// 无过期时间的项 ttl 为 0。
func (c *lruCache) GetWithExpiration(key string) (Value, time.Duration, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	elem, ok := c.items[key]
	if !ok {
		return nil, 0, false
	}

	// 检查是否过期
	now := time.Now()
	if expTime, hasExp := c.expires[key]; hasExp {
		if now.After(expTime) {
			// 已过期
			return nil, 0, false
		}

		// 计算剩余过期时间
		ttl := expTime.Sub(now)
		c.list.MoveToBack(elem)
		return elem.Value.(*lruEntry).value, ttl, true
	}

	// 无过期时间
	c.list.MoveToBack(elem)
	return elem.Value.(*lruEntry).value, 0, true
}

// GetExpiration 返回指定键的过期时间。若键不存在或永不过期，ok 为 false。
func (c *lruCache) GetExpiration(key string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	expTime, ok := c.expires[key]
	return expTime, ok
}

// UpdateExpiration 更新指定键的过期时间。键不存在时返回 false。
// expiration <= 0 表示取消过期时间（变为永不过期）。
func (c *lruCache) UpdateExpiration(key string, expiration time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; !ok {
		return false
	}

	if expiration > 0 {
		c.expires[key] = time.Now().Add(expiration)
	} else {
		delete(c.expires, key)
	}

	return true
}

// UsedBytes 返回当前缓存已使用的字节数（键长度 + 值 Len() 之和）。
func (c *lruCache) UsedBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.usedBytes
}

// MaxBytes 返回缓存配置的最大允许字节数。
func (c *lruCache) MaxBytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.maxBytes
}

// SetMaxBytes 动态调整缓存最大字节数，若当前使用量超过新上限则立即触发淘汰。
func (c *lruCache) SetMaxBytes(maxBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes
	if maxBytes > 0 {
		c.evict()
	}
}
