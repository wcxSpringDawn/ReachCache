/*
Copyright 2026 wcxSpringDawn

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

// 本文件实现了 Cache 结构体——对底层 store.Store 的并发安全封装。
//
// Cache 在 Store 之上提供三个关键能力：
//   - 懒加载初始化：NewCache 仅保存配置，首次 Add 时才通过双重检查锁定分配 Store
//   - 并发安全外壳：使用 sync.RWMutex 保护 store 实例的访问
//   - 统计信息收集：基于 atomic 的 hits/misses 计数和命中率计算
//
// 锁层级关系（由上到下）：
//   Cache.mu (sync.RWMutex) → 保护 store 实例的引用和状态
//   lruCache.mu / lru2Store.locks[i] → Store 内部的数据竞争控制
// 两层锁互不干涉，核心理念是缩小锁粒度。

package reachcache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/wcxSpringDawn/ReachCache/store"
)

// Cache 是对底层 store.Store 的并发安全封装，提供懒加载初始化、ByteView 类型适配和统计信息。
//
// 锁约定：
//   - 读锁（RLock）：保护 Get / Add / AddWithExpiration / Delete / Len
//   - 写锁（Lock）：保护 Clear / Close（含 ensureInitialized 中的首次初始化）
//
// 生命周期：
//   - 创建：NewCache(opts) → 仅保存配置，不分配 Store
//   - 首次写入：Add → ensureInitialized() → 双重检查锁定创建 Store
//   - 关闭：Close() → CompareAndSwap 关闭标记 → 释放 Store → 重置 initialized
type Cache struct {
	mu          sync.RWMutex
	store       store.Store
	opts        CacheOptions
	hits        int64
	misses      int64
	initialized int32 // 原子变量：0=未初始化，1=已初始化
	closed      int32 // 原子变量：0=运行中，1=已关闭
}

// CacheOptions 是创建 Cache 时的配置项，直接映射到 store.Options。
// 可通过 DefaultCacheOptions() 获取推荐默认值。
type CacheOptions struct {
	CacheType    store.CacheType                     // LRU 或 LRU2
	MaxBytes     int64                               // 最大内存字节数
	BucketCount  uint16                              // 桶数量（LRU-2 使用）
	CapPerBucket uint16                              // 每桶 L1 容量（LRU-2 使用）
	Level2Cap    uint16                              // 每桶 L2 容量（LRU-2 使用）
	CleanupTime  time.Duration                       // 过期清理间隔
	OnEvicted    func(key string, value store.Value) // 淘汰回调
}

// DefaultCacheOptions 返回推荐默认配置：LRU-2、8MB 上限、16 桶、512/256 两级容量、1 分钟清理间隔。
func DefaultCacheOptions() CacheOptions {
	return CacheOptions{
		CacheType:    store.LRU2,
		MaxBytes:     8 * 1024 * 1024, // 8MB
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    256,
		CleanupTime:  time.Minute,
		OnEvicted:    nil,
	}
}

// NewCache 创建一个未初始化的 Cache 实例（仅保存配置）。
// 底层 Store 在首次 Add 时通过 ensureInitialized 懒加载创建。
func NewCache(opts CacheOptions) *Cache {
	return &Cache{
		opts: opts,
	}
}

// ensureInitialized 使用双重检查锁定（Double-Checked Locking）确保 Store 只初始化一次。
//
// 第一次检查：atomic.LoadInt32 无锁快速路径，绝大多数请求在此返回
// 第二次检查：获取写锁后确认 initialized==0，防止多 goroutine 重复初始化
func (c *Cache) ensureInitialized() {
	// 快速检查缓存是否已初始化，避免不必要的锁争用
	if atomic.LoadInt32(&c.initialized) == 1 {
		return
	}

	// 双重检查锁定模式
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.initialized == 0 {
		// 创建存储选项
		storeOpts := store.Options{
			MaxBytes:        c.opts.MaxBytes,
			BucketCount:     c.opts.BucketCount,
			CapPerBucket:    c.opts.CapPerBucket,
			Level2Cap:       c.opts.Level2Cap,
			CleanupInterval: c.opts.CleanupTime,
			OnEvicted:       c.opts.OnEvicted,
		}

		// 创建存储实例
		c.store = store.NewStore(c.opts.CacheType, storeOpts)

		// 标记为已初始化
		atomic.StoreInt32(&c.initialized, 1)

		logrus.Infof("Cache initialized with type %s, max bytes: %d", c.opts.CacheType, c.opts.MaxBytes)
	}
}

// Get 从缓存中获取值。
//
// 执行路径：
//  1. 检查 closed：已关闭 → 返回 (ByteView{}, false)
//  2. 检查 initialized：未初始化 → misses++ → 返回 (ByteView{}, false)
//  3. 获取读锁 → store.Get(key)
//  4. 命中 → hits++ → 类型断言为 ByteView → 返回
//  5. 未命中或类型断言失败 → misses++ → 返回 (ByteView{}, false)
//
// 注意 Get 不会触发懒初始化——未初始化的 Cache 永远返回未命中。
func (c *Cache) Get(ctx context.Context, key string) (value ByteView, ok bool) {
	if atomic.LoadInt32(&c.closed) == 1 {
		return ByteView{}, false
	}

	// 如果缓存未初始化，直接返回未命中
	if atomic.LoadInt32(&c.initialized) == 0 {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// 从底层存储获取
	val, found := c.store.Get(key)
	if !found {
		atomic.AddInt64(&c.misses, 1)
		return ByteView{}, false
	}

	// 更新命中计数
	atomic.AddInt64(&c.hits, 1)

	// 类型断言并返回
	if bv, ok := val.(ByteView); ok {
		return bv, true
	}

	// 类型断言失败
	logrus.Warnf("Type assertion failed for key %s, expected ByteView", key)
	atomic.AddInt64(&c.misses, 1)
	return ByteView{}, false
}

// Add 向缓存中写入 ByteView，永不过期。若 Cache 已关闭则静默忽略。
// 首次 Add 会触发 ensureInitialized 懒加载创建底层 Store。
func (c *Cache) Add(key string, value ByteView) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.store.Set(key, value); err != nil {
		logrus.Warnf("Failed to add key %s to cache: %v", key, err)
	}
}

// AddWithExpiration 向缓存中写入带过期时间的 ByteView。
// 若 expirationTime 已过期则跳过写入。
func (c *Cache) AddWithExpiration(key string, value ByteView, expirationTime time.Time) {
	if atomic.LoadInt32(&c.closed) == 1 {
		logrus.Warnf("Attempted to add to a closed cache: %s", key)
		return
	}

	c.ensureInitialized()

	// 计算过期时间
	expiration := time.Until(expirationTime)
	if expiration <= 0 {
		logrus.Debugf("Key %s already expired, not adding to cache", key)
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	// 设置到底层存储
	if err := c.store.SetWithExpiration(key, value, expiration); err != nil {
		logrus.Warnf("Failed to add key %s to cache with expiration: %v", key, err)
	}
}

// Delete 从缓存中删除指定 key。已关闭或未初始化的 Cache 返回 false。
func (c *Cache) Delete(key string) bool {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Delete(key)
}

// Len 返回当前缓存中的有效条目数。已关闭或未初始化的 Cache 返回 0。
func (c *Cache) Len() int {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.store.Len()
}

// Clear 清空缓存中的所有数据并重置 hits/misses 统计。
// 已关闭或未初始化的 Cache 静默忽略。使用写锁（Lock）确保期间无并发读写。
func (c *Cache) Clear() {
	if atomic.LoadInt32(&c.closed) == 1 || atomic.LoadInt32(&c.initialized) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.store.Clear()

	// 重置统计信息
	atomic.StoreInt64(&c.hits, 0)
	atomic.StoreInt64(&c.misses, 0)
}

// Close 关闭缓存。使用 CompareAndSwap 保证幂等。
// 关闭动作：CAS 设 closed=1 → 写锁 → 关闭底层 Store → store=nil → initialized=0。
// 注意 Close 不自动清空数据，调用前应先 Clear 以触发 OnEvicted 回调。
func (c *Cache) Close() {
	// 如果已经关闭，直接返回
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// store.Close()只是关闭了底层存储的定时清理协程
	if c.store != nil {
		if closer, ok := c.store.(interface{ Close() }); ok {
			closer.Close()
		}
		c.store = nil
	}

	// 重置缓存状态
	atomic.StoreInt32(&c.initialized, 0)

	logrus.Debugf("Cache closed, hits: %d, misses: %d", atomic.LoadInt64(&c.hits), atomic.LoadInt64(&c.misses))
}

// Stats 返回缓存统计信息：initialized、closed、hits、misses、size、hit_rate。
// 用于生产环境监控缓存效率和诊断命中率下降问题。
func (c *Cache) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"initialized": atomic.LoadInt32(&c.initialized) == 1,
		"closed":      atomic.LoadInt32(&c.closed) == 1,
		"hits":        atomic.LoadInt64(&c.hits),
		"misses":      atomic.LoadInt64(&c.misses),
	}

	if atomic.LoadInt32(&c.initialized) == 1 {
		stats["size"] = c.Len()

		// 计算命中率
		totalRequests := stats["hits"].(int64) + stats["misses"].(int64)
		if totalRequests > 0 {
			stats["hit_rate"] = float64(stats["hits"].(int64)) / float64(totalRequests)
		} else {
			stats["hit_rate"] = 0.0
		}
	}

	return stats
}
