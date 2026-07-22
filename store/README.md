# store — 本地缓存存储引擎

## 目录

- [store — 本地缓存存储引擎](#store--本地缓存存储引擎)
  - [目录](#目录)
  - [1. 概述](#1-概述)
  - [2. 接口设计](#2-接口设计)
    - [2.1 Store 接口](#21-store-接口)
    - [2.2 Value 接口](#22-value-接口)
    - [2.3 策略模式与工厂函数](#23-策略模式与工厂函数)
  - [3. 配置选项](#3-配置选项)
  - [4. LRU 淘汰算法](#4-lru-淘汰算法)
    - [4.1 数据结构](#41-数据结构)
    - [4.2 Get 操作：O(1) + 二段锁](#42-get-操作o1--二段锁)
    - [4.3 Set 操作：更新与淘汰](#43-set-操作更新与淘汰)
    - [4.4 过期时间管理](#44-过期时间管理)
    - [4.5 驱逐回调机制](#45-驱逐回调机制)
    - [4.6 LRU 的局限性：缓存污染问题](#46-lru-的局限性缓存污染问题)
  - [5. LRU-2 淘汰算法](#5-lru-2-淘汰算法)
    - [5.1 两级缓存架构](#51-两级缓存架构)
    - [5.2 Get 请求的查找路径](#52-get-请求的查找路径)
    - [5.3 分段锁机制](#53-分段锁机制)
    - [5.4 自适应全局时钟](#54-自适应全局时钟)
    - [5.5 索引化双向链表：底层 cache 实现](#55-索引化双向链表底层-cache-实现)
    - [5.6 后台过期清理与关闭](#56-后台过期清理与关闭)
  - [6. 单元测试覆盖](#6-单元测试覆盖)
    - [store_test.go](#store_testgo)
    - [lru_test.go](#lru_testgo)
    - [lru2_test.go](#lru2_testgo)
  - [7. 使用示例](#7-使用示例)
    - [7.1 创建 LRU 缓存](#71-创建-lru-缓存)
    - [7.2 创建 LRU-2 缓存](#72-创建-lru-2-缓存)
    - [7.3 LRU vs LRU-2 选择建议](#73-lru-vs-lru-2-选择建议)

---

## 1. 概述

`store` 包是 ReachCache 系统的**存储引擎层**，负责数据的内存存储、过期管理和淘汰驱逐。

核心设计原则：

- **接口抽象**：通过 `Store` 接口定义统一的缓存操作契约，上层代码无需关心底层淘汰算法的具体实现。
- **策略可替换**：支持 LRU 和 LRU-2 两种淘汰算法，通过工厂函数 `NewStore` 按需创建，符合开闭原则。
- **高并发优化**：LRU 使用二段锁（Two-Phase Locking）降低读锁持有时间；LRU-2 使用分段锁（Striped Locking）将锁竞争分散到多个桶。
- **GC 友好**：LRU-2 底层的 `cache` 结构体使用基于 `uint16` 索引的双向链表替代指针链表，消除指针类型的 GC 扫描压力。

| 文件       | 职责                                                                    |
| ---------- | ----------------------------------------------------------------------- |
| `store.go` | 定义 `Store` 与 `Value` 接口，提供 `Options` 配置与 `NewStore` 工厂函数 |
| `lru.go`   | 基于 `container/list` + `map` 的标准 LRU 实现，O(1) 访问                |
| `lru2.go`  | 分段锁 + 两级缓存的 LRU-2 实现，提升高并发性能与抗缓存污染能力          |

---

## 2. 接口设计

### 2.1 Store 接口

```go
type Store interface {
    Get(key string) (Value, bool)
    Set(key string, value Value) error
    SetWithExpiration(key string, value Value, expiration time.Duration) error
    Delete(key string) bool
    Clear()
    Len() int
    Close()
}
```

接口遵循**最小完备原则**——每个方法语义明确、不可再拆分：

| 方法                | 参数                      | 返回值          | 说明                                                                  |
| ------------------- | ------------------------- | --------------- | --------------------------------------------------------------------- |
| `Get`               | `key string`              | `(Value, bool)` | 根据键查找对应值。`(value, true)` 命中，`(nil, false)` 未命中或已过期 |
| `Set`               | `key string, value Value` | `error`         | 写入键值对，永不过期。键已存在则更新值                                |
| `SetWithExpiration` | `key, value, expiration`  | `error`         | 与 `Set` 类似，额外支持设置数据存活时间（TTL）                        |
| `Delete`            | `key string`              | `bool`          | 移除指定键及其值。返回是否成功删除                                    |
| `Clear`             | —                         | —               | 清空所有数据，触发每个被清除项的 `OnEvicted` 回调                     |
| `Len`               | —                         | `int`           | 返回当前有效条目数量                                                  |
| `Close`             | —                         | —               | 关闭缓存，释放后台资源（定时器与 goroutine）                          |

`Set` 与 `SetWithExpiration` 的分离设计使永不过期的场景避免了不必要的过期时间参数传递与判断开销。

### 2.2 Value 接口

缓存系统需要精确追踪内存使用量以实现容量限制，因此所有存入缓存的值都必须实现 `Value` 接口：

```go
type Value interface {
    Len() int
}
```

`Len()` 方法返回值对象占用的内存字节数。该设计采用**策略模式**思想：每种值类型自行决定如何计算内存占用，存储引擎无需了解具体类型的内部结构。

ReachCache 中的 `ByteView` 类型实现了该接口：

```go
type ByteView struct {
    b []byte
}

func (b ByteView) Len() int { return len(b.b) }
```

当计算缓存内存使用量时，系统将键的字节长度与值的 `Len()` 返回值相加，累积到 `usedBytes` 字段中。超过 `maxBytes` 上限时触发逐出操作。

### 2.3 策略模式与工厂函数

系统通过 `CacheType` 枚举与 `NewStore` 工厂函数实现策略模式：

```go
type CacheType string

const (
    LRU  CacheType = "lru"
    LRU2 CacheType = "lru2"
)

func NewStore(cacheType CacheType, opts Options) Store {
    switch cacheType {
    case LRU2:
        return newLRU2Cache(opts)
    case LRU:
        return newLRUCache(opts)
    default:
        return newLRUCache(opts) // 未知类型默认 LRU
    }
}
```

上层代码只需指定 `CacheType` 和 `Options` 即可自由切换底层淘汰策略，无需修改业务逻辑。未来扩展新的淘汰算法（如 LFU、ARC 等）仅需实现 `Store` 接口并在工厂函数中注册，符合**开闭原则**——对扩展开放，对修改关闭。

---

## 3. 配置选项

```go
type Options struct {
    MaxBytes        int64           // 最大缓存字节数（LRU 使用）
    BucketCount     uint16          // 桶数量（LRU-2 使用，必须是 2 的幂次方以启用位运算取模）
    CapPerBucket    uint16          // 每桶一级缓存（L1）容量（LRU-2 使用）
    Level2Cap       uint16          // 每桶二级缓存（L2）容量（LRU-2 使用）
    CleanupInterval time.Duration   // 定期清理过期项的间隔
    OnEvicted       func(key string, value Value) // 驱逐回调
}
```

默认值（通过 `NewOptions()` 获取）：

| 配置项            | 默认值        | 适用算法    |
| ----------------- | ------------- | ----------- |
| `MaxBytes`        | `8192`        | LRU         |
| `BucketCount`     | `16`          | LRU-2       |
| `CapPerBucket`    | `512`         | LRU-2       |
| `Level2Cap`       | `256`         | LRU-2       |
| `CleanupInterval` | `time.Minute` | LRU / LRU-2 |
| `OnEvicted`       | `nil`         | LRU / LRU-2 |

---

## 4. LRU 淘汰算法

LRU（Least Recently Used）算法的核心思想是：当缓存满时，优先淘汰最长时间未被访问的数据。

### 4.1 数据结构

系统采用**双向链表 + 哈希表**的经典组合实现 O(1) 时间复杂度的访问与更新：

```go
type lruCache struct {
    mu              sync.RWMutex
    list            *list.List                  // 双向链表维护访问顺序
    items           map[string]*list.Element    // 哈希表：key → 链表节点
    expires         map[string]time.Time        // 独立的过期时间映射
    maxBytes        int64                       // 内存上限
    usedBytes       int64                       // 当前使用量
    onEvicted       func(key string, value Value)
    cleanupInterval time.Duration
    cleanupTicker   *time.Ticker
    closeCh         chan struct{}
}
```

链表节点存储 `lruEntry`：

```go
type lruEntry struct {
    key   string
    value Value
}
```

**链表约定**：

- 链表**头部**（`Front()`）存储**最久未被访问**的数据
- 链表**尾部**（`Back()`）存储**最近被访问**的数据
- 每次访问/更新/插入，将节点移到链表尾部
- 需要淘汰时从链表头部移除

**独立过期时间映射**的设计考量：`expires` 是从键到过期时间的独立映射，而非嵌入链表节点。好处是过期检查时 O(1) 查询，清理过期项可直接在 `expires` 上迭代，与 LRU 淘汰逻辑解耦。

### 4.2 Get 操作：O(1) + 二段锁

```text
Get(key)
  │
  ├─① items[key] 哈希查找 → 不存在 → 返回 (nil, false)
  │
  └─② expires[key] 过期检查 → 已过期 → Delete → 返回 (nil, false)
       │
       └─③ list.MoveToBack(elem) → 返回 (value, true)
```

- **步骤①**：通过 `items` 哈希表 O(1) 定位链表节点，无需遍历链表。
- **步骤②**：通过独立的 `expires` 映射 O(1) 检查过期。过期数据立即同步删除。
- **步骤③**：`list.MoveToBack` 在双向链表中仅涉及常数个指针重连，O(1)。

**二段锁（Two-Phase Locking）优化**：

```go
func (c *lruCache) Get(key string) (Value, bool) {
    // 第一阶段：读锁
    c.mu.RLock()
    elem, ok := c.items[key]
    // ... 过期检查 ...
    entry := elem.Value.(*lruEntry)
    value := entry.value
    c.mu.RUnlock()

    // 第二阶段：写锁（仅更新链表位置）
    c.mu.Lock()
    if _, ok := c.items[key]; ok { // 二次检查防止并发删除
        c.list.MoveToBack(elem)
    }
    c.mu.Unlock()

    return value, true
}
```

设计精妙之处：

1. **读读不互斥**：多个 `Get` 可同时在第一阶段执行查找，仅在第二阶段写链表时才互斥。
2. **锁持有时间最小化**：写锁仅保护链表指针的局部修改，而非整个 `Get` 全过程。
3. **容错处理**：释放读锁到获取写锁的微小窗口内，元素可能被其他协程删除。第二阶段二次检查 `items[key]` 是否存在。

> `Set`、`Delete` 等写操作直接使用 `Lock` 全程保护，避免二段锁引入的竞态复杂度。

### 4.3 Set 操作：更新与淘汰

```text
Set(key, value)
  │
  ├─① 键已存在 → 更新值，MoveToBack，更新 usedBytes
  │
  └─② 键不存在 → 创建 lruEntry，PushBack，更新 items 与 usedBytes
       │
       └─③ evict() → 检查 usedBytes 是否超过 maxBytes
            │
            └─④ 超过 → 循环移除链表头部节点，直到低于上限
```

- `evict()` 方法首先遍历 `expires` 清理过期项，然后从链表头部反复移除节点直到 `usedBytes < maxBytes`。
- 每次 `Set` 后触发的淘汰量通常为 1~2 个节点，平均时间复杂度接近 O(1)。
- 当 `value == nil` 时，`SetWithExpiration` 等同于 `Delete`——这是一种便捷的语义约定。
- 传入 `expiration=0` 时，删除 `expires` 中的记录，缓存项变为永不过期。

驱逐时统一调用 `removeElement` 方法，集中处理链表、哈希表和过期映射的删除，并触发 `onEvicted` 回调：

```go
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
```

### 4.4 过期时间管理

缓存支持在 `SetWithExpiration` 时设置独立的过期时间，存储于独立的 `expires` 映射中。

**惰性删除 + 定期清理**相结合的过期策略：

| 机制         | 触发时机                     | 行为                                   |
| ------------ | ---------------------------- | -------------------------------------- |
| **惰性删除** | `Get` 时发现已过期           | 调用 `Delete`                          |
| **定期清理** | 后台 goroutine（默认每分钟） | 遍历 `expires` 清理过期项 + 超内存淘汰 |

后台清理协程：

```go
func (c *lruCache) cleanupLoop() {
    for {
        select {
        case <-c.cleanupTicker.C:
            c.mu.Lock()
            c.evict() // 遍历 expiries 清过期 + 超容量淘汰
            c.mu.Unlock()
        case <-c.closeCh:
            return
        }
    }
}
```

### 4.5 驱逐回调机制

`OnEvicted` 回调在以下三种场景触发：

1. **容量淘汰**：缓存达到 `maxBytes` 上限，`evict()` 移除链表头部节点
2. **清理过期项**：`evict()` 遍历 `expires` 发现过期项并移除
3. **主动清空**：`Clear()` 遍历所有节点逐个移除

### 4.6 LRU 的局限性：缓存污染问题

标准 LRU 存在**缓存污染（Cache Pollution）**缺陷。

典型场景：系统每隔一定时间执行批量数据扫描（如每日报表），扫描数万条日志记录，这些数据被从数据源写入到缓存，在 LRU 中会被全部推到链表尾部，将原本的热数据挤出缓存，但这些冷数据未来几乎不会再被访问。

> 一批一次性访问的冷数据淹没了一个缓存的热数据。

LRU-2 算法正是针对此问题设计的改进方案——不应仅凭一次访问就将数据标记为"热数据"，而应通过多次访问的频次来筛选。

---

## 5. LRU-2 淘汰算法

LRU-2 的核心思想：**新数据先进入较小的"频次过滤器"缓存（L1），只有被二次访问后才晋升到主要的"热数据"缓存（L2）**。仅被扫描一次的大批量冷数据将永远停留在 L1 并被迅速淘汰，不会污染 L2 中的热数据。

### 5.1 两级缓存架构

```go
type lru2Store struct {
    locks       []sync.Mutex       // 分桶锁数组
    caches      [][2]*cache        // caches[i][0]=一级缓存(L1)，caches[i][1]=二级缓存(L2)，cache是一个用索引化双向链表实现的固定容量的LRU
    onEvicted   func(key string, value Value)
    cleanupTick *time.Ticker
    closeCh     chan struct{}
    closeOnce   sync.Once
    mask        int32              // 位运算掩码
}
```

| 层级               | 命名       | 容量配置                   | 作用                                   |
| ------------------ | ---------- | -------------------------- | -------------------------------------- |
| **L1（一级缓存）** | 频次过滤器 | `CapPerBucket`（默认 512） | 新数据的首次落脚点，充当准入过滤器     |
| **L2（二级缓存）** | 热数据存储 | `Level2Cap`（默认 256）    | 被二次访问的数据晋升至此，相对长期驻留 |

### 5.2 Get 请求的查找路径

```text
Get(key)
  │
  ├─① 查找 L1（频次过滤器）
  │    │
  │    ├─ 找到且未过期 → ② 从 L1 删除该节点
  │    │                   │
  │    │                   └─③ 将数据 put 到 L2（热数据存储），返回
  │    │
  │    └─ 未找到或已过期 → ④ 查找 L2
  │                         │
  │                         ├─ 找到且未过期 → 返回
  │                         │
  │                         └─ 未找到或已过期 → 返回 nil, false
```

核心行为模式：

- **首次 Set**：数据进入 L1，作为"候选者"。
- **首次 Get**：数据在 L1 中命中（被二次访问），从 L1 删除并晋升至 L2。
- **后续 Get**：数据在 L2 中命中，直接返回。
- **抗污染能力**：冷数据只被 Set 一次 → 停留在 L1 → 很快被淘汰；热数据被多次 Get → 晋升 L2 → 长期驻留。缓存命中率在批量扫描等场景下更加稳定。

`Set` 操作始终将数据写入 L1：

```go
func (s *lru2Store) SetWithExpiration(key string, value Value, expiration time.Duration) error {
    idx := hashBKRD(key) & s.mask
    s.locks[idx].Lock()
    _, evictedKey, evictedValue, evicted := s.caches[idx][0].put(key, value, expireAt)
    s.locks[idx].Unlock()
    if evicted && s.onEvicted != nil {
        s.onEvicted(evictedKey, evictedValue)
    }
    return nil
}
```

### 5.3 分段锁机制

高并发下单一锁成为瓶颈。LRU-2 采用**分段锁（Striped Locking）**将全局锁拆分为 N 个独立的桶锁：

```go
locks:  make([]sync.Mutex, mask+1),   // N 个独立的互斥锁
caches: make([][2]*cache, mask+1),    // N 个桶，每桶含 L1 和 L2
```

**BKDR 哈希算法**将 key 映射到桶：

```go
func hashBKRD(s string) (hash int32) {
    for i := 0; i < len(s); i++ {
        hash = hash*131 + int32(s[i])
    }
    return hash
}
```

BKDR 以 131 为基数逐字符累加，计算简单、分布均匀。

**位运算取模**：桶数量为 2 的幂次方时，`hash & mask` 等价于 `hash % BucketCount`，但 `AND` 指令仅 1 个 CPU 周期，远快于 `DIV`。

```go
func maskOfNextPowOf2(cap uint16) uint16 {
    if cap > 0 && cap&(cap-1) == 0 {
        return cap - 1 // 已是 2 的幂次方
    }
    cap |= cap >> 1
    cap |= cap >> 2
    cap |= cap >> 4
    return cap | (cap >> 8) // uint16 最多右移 8 位
}
```

默认 16 个桶，理论上并发度提升 16 倍。对于均匀分布的 key，锁冲突概率降至单锁方案的 1/16。

### 5.4 自适应全局时钟

缓存过期判断中 `time.Now()` 是高频操作，涉及系统调用和 GC 压力。LRU-2 采用**自适应全局时钟**：

```go
var clock = time.Now().UnixNano()

func Now() int64 { return atomic.LoadInt64(&clock) }

func init() {
    go func() {
        for {
            atomic.StoreInt64(&clock, time.Now().UnixNano()) // 每秒校准一次
            for i := 0; i < 9; i++ {
                time.Sleep(100 * time.Millisecond)
                atomic.AddInt64(&clock, int64(100*time.Millisecond)) // 自旋累加
            }
            time.Sleep(100 * time.Millisecond)
        }
    }()
}
```

工作原理：

- 后台 goroutine 每秒通过 `time.Now().UnixNano()` 精准校准一次
- 随后每 100ms 自旋累加 100ms 的纳秒增量（共 9 次）
- 缓存操作通过 `atomic.LoadInt64(&clock)` 原子读取，仅一条 `LOAD` 指令

**一次系统调用 + 九次自旋累加**的模式将 `time.Now()` 的调用频率降低了 90%。对于亚毫秒级的精度偏差，缓存过期判断（通常秒/分钟粒度）完全可接受。

### 5.5 索引化双向链表：底层 cache 实现

LRU-2 中每桶的 L1 和 L2 都是底层 `cache` 结构体的实例。这是 LRU-2 性能优势的核心——使用**基于 `uint16` 索引的双向链表**替代 `container/list`，彻底消除指针的 GC 扫描开销，实现零内存分配。

**设计动机**：`container/list.Element` 包含 `prev *Element, next *Element, Value interface{}`，指针密集给 GC 带来显著扫描压力，`interface{}` 导致值装箱和堆分配。

```go
type cache struct {
    dlnk [][2]uint16       // 索引化双向链表，dlnk[i][0]=前驱，dlnk[i][1]=后继
    m    []node            // 预分配节点数组，固定容量，永不缩扩容
    hmap map[string]uint16 // key → 节点索引（1-based，0 为哨兵）
    last uint16            // 已分配节点数量
}

type node struct {
    k        string
    v        Value
    expireAt int64  // 过期时间戳，0 = 逻辑删除
}
```

**链表索引约定**：

- `dlnk[0]` 是**哨兵节点**（Sentinel），不存储数据
- `dlnk[0][0]`（即 `dlnk[0][p]`）= 链表尾部索引
- `dlnk[0][1]`（即 `dlnk[0][n]`）= 链表头部索引
- `p, n = uint16(0), uint16(1)` 是包级常量，`p`=predecessor，`n`=next

GC 需要跟踪的指针从 `container/list` 的 O(N) 降为 O(1)（仅 map 内部的指针）。

**put 操作（插入与淘汰）**：

| 情况           | 触发条件         | 行为                                                         |
| -------------- | ---------------- | ------------------------------------------------------------ |
| **key 已存在** | `hmap[key]` 命中 | 覆盖 node 中的值和过期时间，`adjust(idx, p, n)` 移到链表头部 |
| **缓存未满**   | `last < cap(m)`  | 分配新索引，插入链表头部                                     |
| **缓存已满**   | `last == cap(m)` | 覆盖链表尾部节点数据，移动到头部（复用而非释放）             |

缓存满时的**覆盖 + 复用**策略是该实现的精妙设计：

- `m` 和 `dlnk` 数组大小始终保持固定，运行期间零分配
- `hmap` 桶数量基本稳定，无需频繁扩容/缩容
- 不删除节点、不释放内存，直接覆盖数据并继承索引

**get 操作**：命中后通过 `adjust(idx, p, n)` 将节点移到链表头部（最近使用位置）。返回指向 `m` 内部元素的指针，仅在持有锁期间有效。

**del 操作（逻辑删除）**：

```go
func (c *cache) del(key string) (*node, int, int64) {
    if idx, ok := c.hmap[key]; ok && c.m[idx-1].expireAt > 0 {
        e := c.m[idx-1].expireAt
        c.m[idx-1].expireAt = 0   // 逻辑删除：expireAt=0
        c.adjust(idx, n, p)       // 移到链表尾部，优先被淘汰
        return &c.m[idx-1], 1, e
    }
    return nil, 0, 0
}
```

采用逻辑删除而非物理删除：不删除链表节点、不删除 `hmap` 条目，仅将 `expireAt` 置 0 并移到尾部。被逻辑删除的节点下次缓存满时将被优先淘汰覆盖。该操作在 `lru2Store.Get` 中用于将数据从 L1 晋升到 L2 时删除 L1 中的副本。

**walk 操作**：从链表头部遍历所有有效节点（跳过 `expireAt=0` 的逻辑删除节点），用于后台清理和 `Len()` 统计。

### 5.6 后台过期清理与关闭

`cleanupLoop` 每个桶独立加锁清理：

1. 对当前桶加锁
2. 遍历 L1 和 L2 收集过期 key
3. 在锁内对过期 key 执行 `delete`
4. 释放锁
5. **在锁外**调用 `OnEvicted` 回调，避免持锁时执行可能耗时的用户代码

`Close` 通过 `sync.Once` 确保只执行一次，停止定时器并关闭信号通道。

---

## 6. 单元测试覆盖

共 **44 个测试用例**，分三个测试文件：

### store_test.go

| 测试                              | 覆盖点                               |
| --------------------------------- | ------------------------------------ |
| `TestNewStore_LRU`                | `NewStore(LRU)` 返回 `*lruCache`     |
| `TestNewStore_LRU2`               | `NewStore(LRU2)` 返回 `*lru2Store`   |
| `TestNewStore_Default`            | 未知类型默认返回 `*lruCache`         |
| `TestNewOptions_Defaults`         | 验证所有配置项默认值                 |
| `TestStoreInterface_Polymorphism` | LRU 和 LRU-2 均正确实现 `Store` 接口 |
| `TestValueInterface`              | `testValue` 编译期满足 `Value` 接口  |

### lru_test.go

| 分类     | 测试                           | 覆盖点                        |
| -------- | ------------------------------ | ----------------------------- |
| 基本读写 | `TestLRU_SetGet`               | Set → Get 命中，数据正确      |
|          | `TestLRU_GetNonExistent`       | 不存在的 key 返回 `false`     |
|          | `TestLRU_UpdateExisting`       | 更新已有 key 的值             |
| 过期     | `TestLRU_SetWithExpiration`    | TTL 过期后 Get 返回 `false`   |
|          | `TestLRU_SetWithoutExpiration` | `expiration=0` 永不过期       |
|          | `TestLRU_SetNilValue`          | `value=nil` 等同于 Delete     |
| 删除     | `TestLRU_Delete`               | 删除存在的 key                |
|          | `TestLRU_DeleteNonExistent`    | 删除不存在的 key 返回 `false` |
| 淘汰     | `TestLRU_Eviction`             | 超 `maxBytes` 时淘汰最旧条目  |
|          | `TestLRU_EvictionOrder`        | 访问过的条目移到尾部不被淘汰  |
| 清理     | `TestLRU_Clear`                | Clear 清空 + OnEvicted 回调   |
|          | `TestLRU_OnEvicted`            | 淘汰时 OnEvicted 回调正确触发 |
| 统计     | `TestLRU_Len`                  | Len 计数正确                  |
|          | `TestLRU_UsedBytes`            | UsedBytes 累计和释放正确      |
| 并发     | `TestLRU_ConcurrentAccess`     | 100 goroutine 并发读写混合    |
|          | `TestLRU_ConcurrentGetSameKey` | 50 goroutine 并发读同一 key   |
| 扩展     | `TestLRU_GetWithExpiration`    | GetWithExpiration 返回 TTL    |
| 生命周期 | `TestLRU_Close`                | Close 安全返回                |

### lru2_test.go

| 分类       | 测试                              | 覆盖点                             |
| ---------- | --------------------------------- | ---------------------------------- |
| 基本读写   | `TestLRU2_SetGet`                 | Set → Get 命中                     |
|            | `TestLRU2_GetNonExistent`         | 不存在的 key 返回 `false`          |
| L1→L2 晋升 | `TestLRU2_L1ToL2Promotion`        | 首次 Get 从 L1 晋升到 L2           |
|            | `TestLRU2_SetOnlyStaysInL1`       | 仅 Set 的数据留在 L1 易被淘汰      |
| 过期       | `TestLRU2_SetWithExpiration`      | TTL 过期后 Get 返回 `false`        |
|            | `TestLRU2_NoExpiration`           | `Set` 默认永不过期                 |
|            | `TestLRU2_PromotedKeyExpires`     | 晋升到 L2 的 key 也能正常过期      |
| 删除       | `TestLRU2_Delete`                 | 删除存在的 key                     |
|            | `TestLRU2_DeleteNonExistent`      | 删除不存在的 key                   |
|            | `TestLRU2_DeletePromotedKey`      | 删除已晋升 L2 的 key               |
| 淘汰       | `TestLRU2_EvictionWhenFull`       | L1 满时淘汰并复用尾部节点          |
|            | `TestLRU2_EvictionCallsOnEvicted` | 淘汰时 OnEvicted 回调触发          |
| 清理       | `TestLRU2_Clear`                  | Clear 清空所有桶                   |
| 统计       | `TestLRU2_Len`                    | Len 跨桶计数，晋升不改变计数       |
| 分桶       | `TestLRU2_BucketDistribution`     | 大量 key 均匀分布到各桶            |
| 并发       | `TestLRU2_ConcurrentAccess`       | 200 goroutine 读写混合             |
|            | `TestLRU2_ConcurrentSameBucket`   | 单桶 50 goroutine 并发（验证桶锁） |
| 生命周期   | `TestLRU2_Close`                  | Close 幂等安全                     |
| 压测       | `TestLRU2_ManyKeys`               | 写入 500 个 key 超总容量           |
| 完整性     | `TestLRU2_DataIntegrity`          | 20 个 key 的读写后取回正确数据     |

---

## 7. 使用示例

### 7.1 创建 LRU 缓存

```go
package main

import (
    "fmt"
    "github.com/vernmorn/reachcache/store"
)

func main() {
    opts := store.NewOptions()
    opts.MaxBytes = 1024 * 1024 // 1MB
    opts.OnEvicted = func(key string, value store.Value) {
        fmt.Printf("evicted: %s\n", key)
    }

    cache := store.NewStore(store.LRU, opts)
    defer cache.Close()

    // 写入（永不过期）
    cache.Set("user:1", &MyValue{data: "Alice"})

    // 写入（10 秒过期）
    cache.SetWithExpiration("session:abc", &MyValue{data: "token123"}, 10*time.Second)

    // 读取
    if v, ok := cache.Get("user:1"); ok {
        fmt.Printf("found: %v\n", v)
    }

    // 获取统计
    fmt.Printf("cache size: %d\n", cache.Len())
}

// MyValue 实现 store.Value 接口
type MyValue struct {
    data string
}

func (v *MyValue) Len() int {
    return len(v.data)
}
```

### 7.2 创建 LRU-2 缓存

```go
cache := store.NewStore(store.LRU2, store.Options{
    BucketCount:  32,    // 32 个桶（并发度 32）
    CapPerBucket: 1024,  // 每桶 L1 容量
    Level2Cap:    512,   // 每桶 L2 容量
    CleanupInterval: 30 * time.Second,
    OnEvicted: func(key string, value store.Value) {
        // 自定义淘汰处理，如释放资源、记录日志
        log.Printf("item evicted: key=%s", key)
    },
})
defer cache.Close()
```

### 7.3 LRU vs LRU-2 选择建议

| 场景                                 | 推荐算法 | 理由                               |
| ------------------------------------ | -------- | ---------------------------------- |
| 热点数据集中、访问具有强时间局部性   | LRU      | 实现简单，O(1) 足够                |
| 存在周期性批量数据扫描（报表、导出） | LRU-2    | L1→L2 晋升门槛防止冷数据污染热数据 |
| 高并发（万级 QPS）                   | LRU-2    | 分段锁并发度更高                   |
| 简单 KV 缓存、数据量小               | LRU      | 配置简单，维护成本低               |
