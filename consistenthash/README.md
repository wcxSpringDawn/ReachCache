# consistenthash — 一致性哈希路由算法

## 目录

- [consistenthash — 一致性哈希路由算法](#consistenthash--一致性哈希路由算法)
  - [目录](#目录)
  - [1. 概述](#1-概述)
  - [2. 一致性哈希环基本原理](#2-一致性哈希环基本原理)
  - [3. 虚拟节点机制](#3-虚拟节点机制)
  - [4. 二分查找路由](#4-二分查找路由)
  - [5. 动态自适应负载均衡](#5-动态自适应负载均衡)
    - [5.1 请求计数采样](#51-请求计数采样)
    - [5.2 不均衡度计算与阈值判定](#52-不均衡度计算与阈值判定)
    - [5.3 动态调整算法](#53-动态调整算法)
    - [5.4 调整时机控制](#54-调整时机控制)
  - [6. 配置选项](#6-配置选项)
  - [7. 在 ReachCache 中的定位](#7-在-reachcache-中的定位)
  - [8. 单元测试覆盖](#8-单元测试覆盖)
  - [9. 使用示例](#9-使用示例)

---

## 1. 概述

`consistenthash` 包实现了一致性哈希路由算法，是 ReachCache 分布式路由层的核心组件。

| 文件                | 职责                                               |
| ------------------- | -------------------------------------------------- |
| `config.go`         | `Config` 配置结构体与 `DefaultConfig` 默认值       |
| `consistenthash.go` | `Map` 哈希环实现：节点增删、key 路由、动态负载均衡 |

核心能力：

- **虚拟节点**：每真实节点 50 个虚拟节点（默认），解决数据倾斜
- **O(log N) 路由**：基于 `sort.Search` 的二分查找
- **动态负载均衡**：每秒检查请求水位，过载节点减少虚拟节点、空闲节点增加

---

## 2. 一致性哈希环基本原理

一致性哈希将整个哈希值空间组织成一个首尾相接的**虚拟环**。每个节点通过哈希函数在环上占据多个位置（虚拟节点）。确定某个 key 归属时，计算 key 的哈希值，在环上**顺时针**查找第一个 ≥ 该值的节点。

```text
哈希环:  0 ────────────────────────────── 2^32-1
              │           │           │
            node-A     node-B     node-C
            (65)       (77)       (90)

key "C" (67): 65 < 67 < 77 → node-B  ← 顺时针查找
key "a" (97): 97 > 90(最大) → 回绕到 node-A(65)
```

当节点加入或离开时，一致性哈希仅影响该节点相邻区间的 key，影响比例约 `1/N`（N 为节点数）。在 50 节点集群中，节点增减仅影响约 2% 的 key——对比取模哈希 `hash(key) % N` 在节点变化时影响近 100% 的 key。

> 这一特性从根本上防止了节点变更引发的大规模缓存失效（缓存雪崩）。

---

## 3. 虚拟节点机制

若真实节点数量较少，哈希函数的随机性可能导致节点在环上的位置不均匀——**数据倾斜**。

虚拟节点（Virtual Nodes）是真实节点在环上的"分身"：

```go
func (m *Map) addNode(node string, replicas int) {
    for i := 0; i < replicas; i++ {
        hash := int(m.config.HashFunc(fmt.Appendf(nil, "%s-%d", node, i)))
        m.keys = append(m.keys, hash)
        m.hashMap[hash] = node
    }
    m.nodeReplicas[node] = replicas
}
```

默认每节点 50 个虚拟节点（`DefaultReplicas=50`），命名格式为 `"nodeName-0"` 到 `"nodeName-49"`。虚拟节点数量越多，环上分布越均匀。虚拟节点同时是动态负载均衡的调整维度——增加/减少虚拟节点 = 增加/减少该节点负责的 key 区间。

---

## 4. 二分查找路由

路由的性能核心在"有序环上查找目标节点"。`Map` 将虚拟节点哈希值存储在有序切片 `keys` 中，使用 `sort.Search` 实现 O(log N) 二分查找：

```go
func (m *Map) Get(key string) string {
    hash := int(m.config.HashFunc([]byte(key)))

    // 二分查找第一个 ≥ hash 的虚拟节点
    idx := sort.Search(len(m.keys), func(i int) bool {
        return m.keys[i] >= hash
    })

    // 边界回绕：hash 大于所有虚拟节点 → 回到环首
    if idx == len(m.keys) {
        idx = 0
    }

    node := m.hashMap[m.keys[idx]]
    atomic.AddInt64(m.nodeCounts[node], 1) // 负载采样
    atomic.AddInt64(&m.totalRequests, 1)
    return node
}
```

当虚拟节点 1000 个时，一次 `Get` 仅需约 10 次比较（log₂1000 ≈ 10）。

**哈希函数**：默认使用 `crc32.ChecksumIEEE`，在 x86-64 上有硬件加速指令（SSE 4.2 `crc32`），单指令完成 4/8 字节计算，吞吐量达数 GB/s。可通过 `Config.HashFunc` 替换为自定义哈希。

---

## 5. 动态自适应负载均衡

静态虚拟节点配置无法应对动态请求模式。即使各节点分配的 key 数量大致相等，热点 key 的存在可能导致某些节点负载远高于其他——**请求倾斜**。

系统通过持续监控各节点请求水位，动态调整虚拟节点配比。

### 5.1 请求计数采样

每个 `Get` 调用通过 `atomic.AddInt64` 记录被命中节点的请求计数：

```go
atomic.AddInt64(m.nodeCounts[node], 1)
atomic.AddInt64(&m.totalRequests, 1)
```

`nodeCounts` 类型为 `map[string]*int64`，存储指针而非值以绕过 Go map value 不可寻址的限制。

### 5.2 不均衡度计算与阈值判定

负载不均衡度定义为：`|节点负载 − 平均负载| / 平均负载`

```go
func (m *Map) checkAndRebalance() {
    if atomic.LoadInt64(&m.totalRequests) < 1000 {
        return // 样本太少，避免噪声
    }

    m.mu.RLock()
    if len(m.nodeReplicas) == 0 { // 空环防护
        m.mu.RUnlock()
        return
    }

    avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))
    var maxDiff float64
    for _, cnt := range m.nodeCounts {
        count := atomic.LoadInt64(cnt)
        diff := math.Abs(float64(count) - avgLoad)
        if diff/avgLoad > maxDiff {
            maxDiff = diff / avgLoad
        }
    }

    needRebalance := maxDiff > m.config.LoadBalanceThreshold
    m.mu.RUnlock()

    if needRebalance {
        m.rebalanceNodes() // 释放读锁后调用，避免锁升级死锁
    }
}
```

> 读取负载分布时持有读锁保护 `nodeReplicas` 和 `nodeCounts` 的并发访问，读取完成后释放读锁再决定是否调用 `rebalanceNodes()`，避免 Go `sync.RWMutex` 锁升级（持有 RLock 时 Lock 会死锁）。

例如 3 节点集群，平均每节点 1000 请求，节点 A 为 1500：不均衡度 = `|1500−1000|/1000 = 50%`，超过默认 25% 阈值，触发调整。

### 5.3 动态调整算法

`rebalanceNodes` 实现"**过载减少、空闲增加**"的调整策略：

| 节点状态 | 负载特征        | 调整方向     | 公式                              |
| -------- | --------------- | ------------ | --------------------------------- |
| 过载     | `loadRatio > 1` | 减少虚拟节点 | `new = current / loadRatio`       |
| 空闲     | `loadRatio < 1` | 增加虚拟节点 | `new = current × (2 − loadRatio)` |
| 平衡     | `loadRatio ≈ 1` | 不变         | `new = current`                   |

数值示例：

- 节点 A 负载为均值 2 倍 → `50 / 2.0 = 25` 虚拟节点（减半，负载下降）
- 节点 B 负载为均值 0.5 倍 → `50 × (2−0.5) = 75` 虚拟节点（增加 50%，负载上升）

**非对称设计**：过载比空闲危害更大，因此过载节点使用除法（等比缩减、更积极），空闲节点使用 `2−ratio`（渐进增加、防过度回调）。

调整完成后重置所有计数器，`sort.Ints(m.keys)` 保证下一轮路由的正确性。

### 5.4 调整时机控制

两级控制：

| 层级       | 机制                          | 说明                                 |
| ---------- | ----------------------------- | ------------------------------------ |
| 样本量过滤 | `totalRequests < 1000` 不调整 | 避免小样本随机噪声导致误调整和环震荡 |
| 定期检查   | 后台 goroutine 每秒一次       | 1~2 秒内响应负载变化，开销可忽略     |

触发公式：`(总请求 ≥ 1000) AND (存在节点不均衡度 > 25%)`

后台 goroutine 通过 `Map.Close()` 优雅退出，调用方（`ClientPicker` 或独立使用者）应在不再需要时调用 `Close()` 防止 goroutine 泄漏。

---

## 6. 配置选项

```go
type Config struct {
    DefaultReplicas      int     // 每节点默认虚拟节点数（默认 50）
    MinReplicas          int     // 调整下限（默认 10）
    MaxReplicas          int     // 调整上限（默认 200）
    HashFunc             func([]byte) uint32 // 哈希函数（默认 CRC32）
    LoadBalanceThreshold float64 // 不均衡度阈值（默认 0.25）
}
```

```go
// 使用自定义配置
m := consistenthash.New(consistenthash.WithConfig(&consistenthash.Config{
    DefaultReplicas:      100,
    MinReplicas:          20,
    MaxReplicas:          500,
    HashFunc:             myHashFunc,
    LoadBalanceThreshold: 0.2,
}))
```

---

## 7. 在 ReachCache 中的定位

`Map` 被 `ClientPicker` 使用，位于分布式路由层：

```text
ClientPicker.PickPeer(key)
  │
  └─ consHash.Get(key) → 节点地址
       │
       ├─ 二分查找 (O(log N))
       ├─ 请求计数采样 (atomic)
       └─ 后台负载均衡 (每秒)
```

配合 etcd 服务发现，`ClientPicker` 监听节点上下线事件，动态调用 `Add`/`Remove` 更新哈希环，实现集群拓扑的实时感知与自适应路由。

---

## 8. 单元测试覆盖

共 **18 个测试用例**：

| 分类     | 测试                       | 覆盖点                           |
| -------- | -------------------------- | -------------------------------- |
| 基本     | `TestAdd_Get`              | 添加节点后 Get 返回非空          |
|          | `TestGet_EmptyKey`         | 空 key 返回 `""`                 |
|          | `TestGet_EmptyRing`        | 空环 Get 返回 `""`               |
|          | `TestAdd_EmptyNodeSkipped` | 空节点名被跳过                   |
|          | `TestAdd_NoArgs`           | 无参数 Add 返回 error            |
| 一致性   | `TestGet_Consistency`      | 同一 key 100 次 Get 返回相同节点 |
|          | `TestGet_DifferentKeys`    | 不同 key 分布到多个节点          |
| Remove   | `TestRemove`               | 移除后该节点不再返回             |
|          | `TestRemove_NonExistent`   | 移除不存在节点返回 error         |
|          | `TestRemove_LastNode`      | 移除最后一个节点后 Get 返回 `""` |
| 虚拟节点 | `TestVirtualNodes`         | 默认 50 个虚拟节点               |
|          | `TestCustomReplicas`       | 自定义虚拟节点数生效             |
|          | `TestAdd_DuplicateNode`    | 重复添加覆盖而非累加             |
| 统计     | `TestGetStats`             | GetStats 返回正常分布            |
| 并发     | `TestConcurrent_Get`       | 200 goroutine 并发 Get           |
|          | `TestConcurrent_AddGet`    | 并发 Add + Get 不崩溃            |
| 哈希     | `TestCustomHashFunc`       | 自定义 HashFunc 生效             |
|          | `TestGet_ClockwiseSearch`  | 顺时针查找 + 边界回绕逻辑        |

---

## 9. 使用示例

```go
package main

import (
    "fmt"
    "github.com/vernmorn/reachcache/consistenthash"
)

func main() {
    m := consistenthash.New()
    defer m.Close() // 停止后台负载均衡 goroutine

    // 添加节点
    m.Add("192.168.1.10:8001", "192.168.1.11:8001", "192.168.1.12:8001")

    // 路由 key 到节点
    node := m.Get("user:12345")
    fmt.Println("user:12345 →", node)

    // 查看负载分布
    stats := m.GetStats()
    for node, ratio := range stats {
        fmt.Printf("%s: %.1f%%\n", node, ratio*100)
    }

    // 节点下线
    m.Remove("192.168.1.10:8001")
}
```
