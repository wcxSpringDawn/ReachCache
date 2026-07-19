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

// Package store 是 ReachCache 的本地缓存存储引擎，提供多种淘汰算法的统一接口。
//
// 核心设计原则：
//   - 接口抽象：通过 Store 接口统一缓存操作契约，上层代码不感知底层实现
//   - 策略可替换：支持 LRU 与 LRU-2 两种淘汰算法，通过 NewStore 工厂按需创建
//   - 高并发优化：LRU 使用二段锁降低读锁持有时间；LRU-2 使用分段锁分散锁竞争
//   - GC 友好：LRU-2 底层使用 uint16 索引化双向链表替代指针链表，消除 GC 扫描压力
//
// 基本用法：
//
//	opts := store.NewOptions()
//	opts.MaxBytes = 64 << 20 // 64MB
//	cache := store.NewStore(store.LRU, opts)
//	defer cache.Close()
//	cache.Set("key", myValue)
//	if v, ok := cache.Get("key"); ok {
//	    fmt.Println(v)
//	}
package store

import "time"

// Value 是存入缓存的值必须实现的接口。
// 每种值类型自行决定如何计算内存占用，存储引擎无需了解具体类型的内部结构。
type Value interface {
	Len() int // Len 返回值对象占用的内存字节数
}

// Store 是缓存存储的统一接口，支持 LRU 和 LRU-2 两种实现。
// 接口遵循最小完备原则，每个方法语义明确、不可再拆分。
type Store interface {
	// Get 根据键查找对应值。返回 (value, true) 表示命中，
	// 返回 (nil, false) 表示未命中或已过期。
	Get(key string) (Value, bool)

	// Set 写入键值对，永不过期。若键已存在则更新值。
	Set(key string, value Value) error

	// SetWithExpiration 写入键值对并设置存活时间（TTL）。
	// expiration 为 0 表示永不过期。
	SetWithExpiration(key string, value Value, expiration time.Duration) error

	// Delete 移除指定键及其值。返回 true 表示成功删除。
	Delete(key string) bool

	// Clear 清空所有缓存数据，并触发每个被清除项的 OnEvicted 回调（如已设置）。
	Clear()

	// Len 返回当前缓存中的有效条目数量。
	Len() int

	// Close 关闭缓存，释放后台资源（定时器与 goroutine）。
	// 关闭后不应再使用该实例。
	Close()
}

// CacheType 表示缓存淘汰算法的类型。
type CacheType string

const (
	// LRU 标准 LRU（Least Recently Used）淘汰算法。
	// 基于 container/list + map，O(1) 访问，适合热点数据集中、时间局部性强的场景。
	LRU CacheType = "lru"

	// LRU2 LRU-2 淘汰算法，采用分段锁 + 两级缓存架构。
	// 新数据先进入 L1（频次过滤器），二次访问后晋升 L2（热数据存储），
	// 有效防止批量冷数据扫描导致的缓存污染，适合高并发场景。
	LRU2 CacheType = "lru2"
)

// Options 是创建缓存实例时的通用配置。
// 不同算法按需读取相关字段，未使用的字段可忽略。
type Options struct {
	// MaxBytes 最大缓存字节数（LRU 使用）。键与值的字节长度之和超过此值触发淘汰。
	MaxBytes int64

	// BucketCount 桶数量（LRU-2 使用）。内部会向上取整为 2 的幂次方以启用位运算取模。
	BucketCount uint16

	// CapPerBucket 每桶一级缓存（L1）的最大节点数（LRU-2 使用）。
	CapPerBucket uint16

	// Level2Cap 每桶二级缓存（L2）的最大节点数（LRU-2 使用）。
	Level2Cap uint16

	// CleanupInterval 定期清理过期项的时间间隔。默认为 1 分钟。
	CleanupInterval time.Duration

	// OnEvicted 淘汰回调函数，在缓存项被淘汰或清空时调用。
	// key 是被淘汰项的键，value 是被淘汰项的值。注意回调在持锁外执行，避免阻塞缓存操作。
	OnEvicted func(key string, value Value)
}

// NewOptions 返回填充了默认值的 Options。
//
// 默认值：
//   - MaxBytes: 8192
//   - BucketCount: 16
//   - CapPerBucket: 512
//   - Level2Cap: 256
//   - CleanupInterval: 1 分钟
//   - OnEvicted: nil
func NewOptions() Options {
	return Options{
		MaxBytes:        8192,
		BucketCount:     16,
		CapPerBucket:    512,
		Level2Cap:       256,
		CleanupInterval: time.Minute,
		OnEvicted:       nil,
	}
}

// NewStore 根据指定的缓存类型和配置创建 Store 实例。
//
// 支持的 cacheType：
//   - LRU: 标准 LRU 实现
//   - LRU2: LRU-2 实现（分段锁 + 两级缓存）
//   - 其他值默认采用 LRU
//
// 返回的 Store 实例是并发安全的，多个 goroutine 可同时读写。
func NewStore(cacheType CacheType, opts Options) Store {
	switch cacheType {
	case LRU2:
		return newLRU2Cache(opts)
	case LRU:
		return newLRUCache(opts)
	default:
		return newLRUCache(opts)
	}
}
