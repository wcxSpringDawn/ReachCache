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

// 本文件实现了 LRU-2 淘汰算法，旨在解决标准 LRU 的缓存污染问题。
//
// 核心设计：
//   - 两级缓存架构：L1（频次过滤器）→ 二次访问后晋升 L2（热数据存储）
//   - 分段锁：通过 BKDR 哈希 + 位运算取模将全局锁拆分为 N 个桶锁
//   - 自适应全局时钟：降低 time.Now() 系统调用频率，减少 GC 压力
//   - 索引化双向链表：基于 uint16 索引替代指针链表，消除 GC 指针扫描开销
//
// 适用场景：存在周期性批量数据扫描（如报表生成、数据导出）的高并发缓存系统。

package store

import (
	"math"
	"sync"
	"sync/atomic"
	"time"
)

// lru2Store 是基于分桶和两级缓存的 LRU-2 实现。
// 每个桶持有独立的互斥锁和一对 cache 实例（L1 频次过滤器 + L2 热数据存储）。
// 桶数量必须是 2 的幂次方，通过位运算 mask 快速定位桶。
type lru2Store struct {
	locks       []sync.Mutex                  // 分桶锁，每个桶独立加锁，减少锁竞争
	caches      [][2]*cache                   // caches[i][0] = L1（频次过滤器），caches[i][1] = L2（热数据存储）
	onEvicted   func(key string, value Value) // 淘汰回调，在持锁外执行
	cleanupTick *time.Ticker
	closeCh     chan struct{}
	closeOnce   sync.Once
	mask        int32 // 桶索引掩码，hash(key) & mask 等价于 hash(key) % BucketCount
}

// newLRU2Cache 创建 LRU-2 缓存实例。
// BucketCount 向上取整为 2 的幂次方以启用位运算取模。
// 若 CapPerBucket、Level2Cap、CleanupInterval 为零值则使用默认值。
func newLRU2Cache(opts Options) *lru2Store {
	// 默认配置
	if opts.BucketCount == 0 {
		// 桶数量必须是 2 的幂次方，以便使用位运算进行索引计算。
		opts.BucketCount = 16
	}
	// 每个桶的一级缓存的node数量
	if opts.CapPerBucket == 0 {
		opts.CapPerBucket = 1024
	}
	// 每个桶的二级缓存的node数量
	if opts.Level2Cap == 0 {
		opts.Level2Cap = 1024
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = time.Minute
	}

	// 计算大于等于BucketCount的最近的2的幂次方的值-1，作为掩码值，用于位运算取模
	mask := maskOfNextPowOf2(opts.BucketCount) // 假设 BucketCount = 16，则 mask = 15 (0b1111)，用于计算桶索引时，hash(key) & mask 可以快速得到桶索引，避免使用取模运算，提高性能。
	s := &lru2Store{
		locks:       make([]sync.Mutex, mask+1),
		caches:      make([][2]*cache, mask+1),
		onEvicted:   opts.OnEvicted,
		cleanupTick: time.NewTicker(opts.CleanupInterval),
		closeCh:     make(chan struct{}),
		mask:        int32(mask),
	}

	// 初始化每个桶的一级和二级缓存
	for i := range s.caches {
		s.caches[i][0] = Create(opts.CapPerBucket)
		s.caches[i][1] = Create(opts.Level2Cap)
	}

	// 启动后台定期清理协程
	if opts.CleanupInterval > 0 {
		go s.cleanupLoop()
	}

	return s
}

// Get 实现 Store 接口。查找路径：
//  1. 检查 L1（频次过滤器）：命中且未过期 → 从 L1 删除 → 晋升到 L2 → 返回
//  2. 检查 L2（热数据存储）：命中且未过期 → 直接返回
//  3. 未命中或已过期 → 返回 (nil, false)
//
// 过期检查使用自适应全局时钟 Now()，精度约 100ms，足以满足缓存过期判断需求。
func (s *lru2Store) Get(key string) (Value, bool) {
	// 位运算计算桶索引，hashBKRD 是一个哈希函数，确保相同的键总是映射到同一个桶
	idx := hashBKRD(key) & s.mask // 等价于hash(key)%BucketCount，但使用位运算更高效
	s.locks[idx].Lock()

	currentTime := Now()
	var cbKey string
	var cbValue Value
	needCallback := false

	// 首先检查一级缓存
	n1, status1, expireAt := s.caches[idx][0].del(key) // 直接从一级缓存中删除项
	if status1 > 0 {
		// 从一级缓存找到项目
		if expireAt > 0 && currentTime >= expireAt {
			// 项目已过期，删除它
			deleted, evictedValue := s.delete(key, idx)
			if deleted && evictedValue != nil {
				cbKey, cbValue, needCallback = key, evictedValue, true
			}
			s.locks[idx].Unlock()
			if needCallback && s.onEvicted != nil {
				s.onEvicted(cbKey, cbValue)
			}
			return nil, false
		}

		// 项目有效，将其移至二级缓存
		_, evictedKey, evictedValue, evicted := s.caches[idx][1].put(key, n1.v, expireAt)
		if evicted {
			cbKey, cbValue, needCallback = evictedKey, evictedValue, true
		}
		s.locks[idx].Unlock()
		if needCallback && s.onEvicted != nil {
			s.onEvicted(cbKey, cbValue)
		}
		return n1.v, true
	}

	// 一级缓存未找到，检查二级缓存
	n2, status2 := s._get(key, idx, 1)
	if status2 > 0 && n2 != nil {
		if n2.expireAt > 0 && currentTime >= n2.expireAt {
			// 项目已过期，删除它
			deleted, evictedValue := s.delete(key, idx)
			if deleted && evictedValue != nil {
				cbKey, cbValue, needCallback = key, evictedValue, true
			}
			s.locks[idx].Unlock()
			if needCallback && s.onEvicted != nil {
				s.onEvicted(cbKey, cbValue)
			}
			return nil, false
		}

		s.locks[idx].Unlock()
		return n2.v, true
	}

	s.locks[idx].Unlock()
	return nil, false
}

// Set 实现 Store 接口。新数据始终写入 L1，永不过期。
func (s *lru2Store) Set(key string, value Value) error {
	return s.SetWithExpiration(key, value, 0)
}

// SetWithExpiration 实现 Store 接口。新数据始终写入 L1 并设置过期时间。
// expiration <= 0 表示永不过期（内部使用 math.MaxInt64 作为哨兵值）。
// 若 L1 已满则淘汰链表尾部节点（即最久未使用或逻辑删除的节点），触发 OnEvicted 回调。
func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
	// expiration<=0 表示不过期，用最大时间戳哨兵值表示
	expireAt := int64(math.MaxInt64)
	if expiration > 0 {
		now := Now()
		delta := int64(expiration.Nanoseconds())
		if delta > 0 && now > math.MaxInt64-delta {
			expireAt = math.MaxInt64
		} else {
			expireAt = now + delta
		}
	}

	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	_, evictedKey, evictedValue, evicted := s.caches[idx][0].put(key, value, expireAt)
	s.locks[idx].Unlock()

	if evicted && s.onEvicted != nil {
		s.onEvicted(evictedKey, evictedValue)
	}

	return nil
}

// Delete 实现 Store 接口。同时从 L1 和 L2 中逻辑删除（expireAt=0）指定键。
// 返回 true 表示至少在一个缓存层级中存在并已删除。
func (s *lru2Store) Delete(key string) bool {
	idx := hashBKRD(key) & s.mask
	s.locks[idx].Lock()
	deleted, evictedValue := s.delete(key, idx)
	s.locks[idx].Unlock()

	if deleted && evictedValue != nil && s.onEvicted != nil {
		s.onEvicted(key, evictedValue)
	}

	return deleted
}

// Clear 实现 Store 接口。遍历所有桶的 L1 和 L2 收集 key，逐个 Delete。
// 每个删除都会触发 OnEvicted 回调（如已设置）。
func (s *lru2Store) Clear() {
	keys := make([]string, 0)
	seen := make(map[string]struct{})

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
			return true
		})

		s.locks[i].Unlock()
	}

	for _, key := range keys {
		s.Delete(key)
	}

	//s.expirations = sync.Map{}
}

// Len 实现 Store 接口。遍历所有桶的 L1 和 L2，跳过逻辑删除节点（expireAt=0）统计有效条目数。
func (s *lru2Store) Len() int {
	count := 0

	for i := range s.caches {
		s.locks[i].Lock()

		s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})
		s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
			count++
			return true
		})

		s.locks[i].Unlock()
	}

	return count
}

// Close 实现 Store 接口。通过 sync.Once 确保只执行一次：停止定时器并关闭信号通道，
// 通知后台清理协程退出。重复调用安全。
func (s *lru2Store) Close() {
	s.closeOnce.Do(func() {
		if s.cleanupTick != nil {
			s.cleanupTick.Stop()
		}
		close(s.closeCh)
	})
}

// clock 是自适应全局时钟，用于降低 time.Now() 系统调用频率和 GC 压力。
// 后台 goroutine 每秒通过 time.Now() 校准一次，随后 9 次每 100ms 自旋累加，
// 将系统调用频率降低 90%。读取时使用 atomic.LoadInt64，仅一条 CPU LOAD 指令。
var clock = time.Now().UnixNano()

// Now 返回自适应全局时钟的当前纳秒级时间戳，精度约 100ms。
// 适用于缓存过期判断（通常为秒/分钟粒度），不适用于需要精确时间的场景。
// 并发安全，可在多个 goroutine 中同时调用。
func Now() int64 { return atomic.LoadInt64(&clock) }

func init() {
	go func() {
		for {
			atomic.StoreInt64(&clock, time.Now().UnixNano()) // 每秒校准一次
			for i := 0; i < 9; i++ {
				time.Sleep(100 * time.Millisecond)
				atomic.AddInt64(&clock, int64(100*time.Millisecond)) // 保持 clock 在一个精确的时间范围内，同时避免频繁的系统调用
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

// hashBKRD 使用 BKDR 算法计算字符串的哈希值。
// 以 131 为基数逐字符累加，分布均匀、碰撞率低，用于将 key 映射到缓存桶。
func hashBKRD(s string) (hash int32) {
	for i := 0; i < len(s); i++ {
		hash = hash*131 + int32(s[i])
	}

	return hash
}

// maskOfNextPowOf2 返回大于等于 cap 的最小 2 的幂次方减一，作为位运算掩码。
// 例如：cap=16 → 15(0b1111)，cap=10 → 15，cap=100 → 127。
// 使得 hash & mask 等价于 hash % BucketCount，但效率远高于除法取模。
func maskOfNextPowOf2(cap uint16) uint16 {
	if cap > 0 && cap&(cap-1) == 0 {
		return cap - 1
	}

	// 通过多次右移和按位或操作，将二进制中最高位的 1 右边的所有位都填充为 1
	cap |= cap >> 1
	cap |= cap >> 2
	cap |= cap >> 4

	return cap | (cap >> 8) // cap是uint16类型，所以最多右移8位即可
}

// node 表示缓存中的一个条目，包含键、值和过期时间戳。
// expireAt 为 0 表示逻辑删除（内存仍保留，等待后续写入时复用覆盖）。
type node struct {
	k        string
	v        Value
	expireAt int64
}

// p 和 n 是索引化双向链表中前驱和后继的列索引。
// dlnk[i][p] = 前驱索引, dlnk[i][n] = 后继索引。
// adjust(idx, p, n) = 移到链表头部, adjust(idx, n, p) = 移到链表尾部。
var p, n = uint16(0), uint16(1)

// cache 是基于 uint16 索引数组的双向链表 LRU 实现，是 LRU-2 每桶中 L1/L2 的底层存储。
// 与 container/list 相比：消除指针的 GC 扫描压力，预分配固定容量实现零运行时分配。
//
// 索引约定：
//   - dlnk[0] 是哨兵节点，dlnk[0][p] 指向链表尾部，dlnk[0][n] 指向链表头部
//   - hmap 中存储的是 1-based 索引（0 保留给哨兵），访问 m 时需 idx-1
//   - last 记录已分配的节点数量，last == cap(m) 时后续写入触发淘汰复用
type cache struct {
	dlnk [][2]uint16
	m    []node
	hmap map[string]uint16
	last uint16
}

// Create 创建一个预分配容量的 cache 实例。dlnk 长度为 cap+1（索引 0 为哨兵），
// m 长度为 cap（节点数固定，运行期间不扩容不缩容）。
func Create(cap uint16) *cache {
	return &cache{
		dlnk: make([][2]uint16, cap+1),
		m:    make([]node, cap),
		hmap: make(map[string]uint16, cap),
		last: 0,
	}
}

// put 向缓存中添加或更新项。
//
// 返回值：
//   - int: 1 表示新增，0 表示更新
//   - string: 被淘汰的 key（如有）
//   - Value: 被淘汰的 value（如有）
//   - bool: 是否发生了淘汰（即尾部节点在有效状态下被覆盖）
//
// 三种情况：
//  1. key 已存在 → 更新值并移到链表头部
//  2. 缓存未满 → 在链表头部插入新节点
//  3. 缓存已满 → 覆盖链表尾部节点（逻辑删除或最久未使用），移到链表头部
func (c *cache) put(key string, val Value, expireAt int64) (int, string, Value, bool) {
	// key已存在，更新值和过期时间，并将节点移动到链表头部
	if idx, ok := c.hmap[key]; ok {
		c.m[idx-1].v, c.m[idx-1].expireAt = val, expireAt
		c.adjust(idx, p, n) // 刷新到链表头部
		return 0, "", nil, false
	}

	// 缓存已满，淘汰并复用尾部节点
	if c.last == uint16(cap(c.m)) {
		tail := &c.m[c.dlnk[0][p]-1]
		evicted := tail.expireAt > 0
		evictedKey := tail.k
		evictedValue := tail.v

		delete(c.hmap, (*tail).k)
		c.hmap[key], (*tail).k, (*tail).v, (*tail).expireAt = c.dlnk[0][p], key, val, expireAt
		// 将复用的节点移动到链表头部
		c.adjust(c.dlnk[0][p], p, n)

		return 1, evictedKey, evictedValue, evicted
	}

	// 缓存未满
	c.last++
	if len(c.hmap) <= 0 {
		c.dlnk[0][p] = c.last // 链表为空时，链表尾部指向新节点
	} else {
		c.dlnk[c.dlnk[0][n]][p] = c.last // 原先链表头部的前驱指向新节点
	}

	// 初始化新节点并更新链表指针
	c.m[c.last-1].k = key
	c.m[c.last-1].v = val
	c.m[c.last-1].expireAt = expireAt
	c.dlnk[c.last] = [2]uint16{0, c.dlnk[0][n]} // 新节点的前驱指向哨兵节点，后继指向当前链表头部
	c.hmap[key] = c.last
	c.dlnk[0][n] = c.last // 更新哨兵节点的后继指针指向新节点

	return 1, "", nil, false
}

// get 从缓存中获取键对应的节点。命中时将节点移到链表头部。
// 返回指向 m 内部元素的指针——该指针仅在持有锁期间有效。
func (c *cache) get(key string) (*node, int) {
	if idx, ok := c.hmap[key]; ok {
		c.adjust(idx, p, n)
		return &c.m[idx-1], 1
	}
	return nil, 0
}

// del 逻辑删除缓存项：将 expireAt 置为 0（删除标记），节点移到链表尾部。
// 不删除 hmap 条目和链表节点——被逻辑删除的节点在后续 put 满时被自然覆盖复用。
// 这种策略避免了 map rehash 和链表断链重连的开销，实现零成本删除。
func (c *cache) del(key string) (*node, int, int64) {
	if idx, ok := c.hmap[key]; ok && c.m[idx-1].expireAt > 0 {
		e := c.m[idx-1].expireAt
		c.m[idx-1].expireAt = 0 // 标记为已删除，这只是一种逻辑删除，实际内存仍然保留，等待后续复用
		c.adjust(idx, n, p)     // 移动到链表尾部，后续优先复用
		return &c.m[idx-1], 1, e
	}

	return nil, 0, 0
}

// walk 遍历缓存中所有有效节点（跳过 expireAt=0 的逻辑删除节点）。
// walker 返回 false 时提前终止遍历。用于后台清理和 Len 统计。
func (c *cache) walk(walker func(key string, value Value, expireAt int64) bool) {
	for idx := c.dlnk[0][n]; idx != 0; idx = c.dlnk[idx][n] {
		if c.m[idx-1].expireAt > 0 && !walker(c.m[idx-1].k, c.m[idx-1].v, c.m[idx-1].expireAt) {
			return
		}
	}
}

// adjust 将节点从当前位置断开并连接到哨兵的另一端，实现"移到头部"或"移到尾部"。
//
// 参数化设计：
//   - adjust(idx, p, n) 即 (idx, 0, 1)：检查前驱是否存在，存在则移到链表头部
//   - adjust(idx, n, p) 即 (idx, 1, 0)：检查后继是否存在，存在则移到链表尾部
//
// f（from）方向无连接时（节点已在目标端），跳过操作。
func (c *cache) adjust(idx, f, t uint16) {
	if c.dlnk[idx][f] != 0 {
		// 将节点从当前位置移除
		c.dlnk[c.dlnk[idx][t]][f] = c.dlnk[idx][f]
		c.dlnk[c.dlnk[idx][f]][t] = c.dlnk[idx][t]

		// 将节点插入到链表头部或尾部
		c.dlnk[idx][f] = 0
		c.dlnk[idx][t] = c.dlnk[0][t]
		c.dlnk[c.dlnk[0][t]][f] = idx
		c.dlnk[0][t] = idx
	}
}

// _get 从指定桶的指定级别缓存（0=L1, 1=L2）中获取节点。
// 同时检查过期——已过期或已逻辑删除（expireAt=0）的节点返回 nil。
func (s *lru2Store) _get(key string, idx, level int32) (*node, int) {
	if n, st := s.caches[idx][level].get(key); st > 0 && n != nil {
		currentTime := Now()
		if n.expireAt <= 0 || currentTime >= n.expireAt {
			// 过期或已删除
			return nil, 0
		}
		return n, st
	}

	return nil, 0
}

// delete 从指定桶的 L1 和 L2 中逻辑删除指定键。返回是否删除成功及被删除的值。
// 调用前必须持有对应桶的锁。
func (s *lru2Store) delete(key string, idx int32) (bool, Value) {
	n1, s1, _ := s.caches[idx][0].del(key)
	n2, s2, _ := s.caches[idx][1].del(key)
	deleted := s1 > 0 || s2 > 0
	var evictedValue Value

	if n1 != nil && n1.v != nil {
		evictedValue = n1.v
	} else if n2 != nil && n2.v != nil {
		evictedValue = n2.v
	}

	if deleted {
		//s.expirations.Delete(key)
	}

	return deleted, evictedValue
}

// cleanupLoop 后台定期清理协程。每个桶独立加锁：
//  1. 遍历 L1 和 L2 收集过期 key
//  2. 在锁内执行 delete（逻辑删除）
//  3. 释放锁后在锁外调用 OnEvicted，避免持锁时执行用户回调
func (s *lru2Store) cleanupLoop() {
	for {
		select {
		case <-s.closeCh:
			return
		case <-s.cleanupTick.C:
			currentTime := Now()

			for i := range s.caches {
				s.locks[i].Lock()

				// 检查并清理过期项目
				expiredKeys := make([]string, 0)
				expiredSeen := make(map[string]struct{})
				evicted := make(map[string]Value)

				// 遍历L1
				s.caches[i][0].walk(func(key string, value Value, expireAt int64) bool {
					if expireAt > 0 && currentTime >= expireAt {
						if _, ok := expiredSeen[key]; !ok {
							expiredSeen[key] = struct{}{}
							expiredKeys = append(expiredKeys, key)
						}
					}
					return true
				})

				// 遍历L2
				s.caches[i][1].walk(func(key string, value Value, expireAt int64) bool {
					if expireAt > 0 && currentTime >= expireAt {
						if _, ok := expiredSeen[key]; !ok {
							expiredSeen[key] = struct{}{}
							expiredKeys = append(expiredKeys, key)
						}
					}
					return true
				})

				// 对遍历收集到的过期键进行删除
				for _, key := range expiredKeys {
					deleted, value := s.delete(key, int32(i))
					if deleted && value != nil {
						evicted[key] = value
					}
				}

				s.locks[i].Unlock()

				// 锁外调用驱逐回调函数，避免在持锁状态下执行可能耗时的操作
				if s.onEvicted != nil {
					for key, value := range evicted {
						s.onEvicted(key, value)
					}
				}
			}
		}
	}
}
