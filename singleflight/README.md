# singleflight — 请求合并机制

## 目录

- [singleflight — 请求合并机制](#singleflight--请求合并机制)
  - [目录](#目录)
  - [1. 概述](#1-概述)
  - [2. 缓存击穿问题](#2-缓存击穿问题)
  - [3. SingleFlight 工作原理](#3-singleflight-工作原理)
  - [4. 核心数据结构](#4-核心数据结构)
  - [5. Do 方法详解](#5-do-方法详解)
    - [5.1 首个请求：创建 call 并执行](#51-首个请求创建-call-并执行)
    - [5.2 后续请求：等待并共享结果](#52-后续请求等待并共享结果)
    - [5.3 defer 保障的资源安全释放](#53-defer-保障的资源安全释放)
  - [6. 竞态窗口：乐观并发控制](#6-竞态窗口乐观并发控制)
  - [7. 在 ReachCache 中的定位](#7-在-reachcache-中的定位)
  - [8. 单元测试覆盖](#8-单元测试覆盖)
  - [9. 使用示例](#9-使用示例)

---

## 1. 概述

`singleflight` 包提供**请求合并（Request Coalescing）**机制，用于防止缓存击穿。

核心思想：对于同一 key 的多个并发请求，仅让第一个实际执行数据加载操作（如数据库查询、计算引擎等），其余请求阻塞等待并共享其结果。

| 文件              | 职责                                 |
| ----------------- | ------------------------------------ |
| `singleflight.go` | `Group` 结构体与 `Do` 方法的完整实现 |

---

## 2. 缓存击穿问题

**缓存击穿（Cache Breakdown）** 是指某一热点 key 在缓存中恰好过期或被删除的瞬间，大量并发请求同时穿透缓存直达后端数据源的现象。

危害体现在三个层面：

1. **后端数据源过载**：热点 key 可能承载每秒数千次查询。缓存失效时，这些请求同时涌向后端数据库，可能导致连接池耗尽、查询超时甚至服务崩溃。典型场景如电商秒杀中热门商品库存 key 过期瞬间。
2. **请求延时飙升**：P99 延迟从缓存命中的微秒级飙升至数据库查询的毫秒甚至秒级。
3. **级联故障风险**：加载线程被大量阻塞，影响同一进程中其他服务的正常响应。

传统的互斥锁方案存在两个问题：锁是协程级别的阻塞，大量 goroutine 在锁上自旋或休眠增加调度负担；全局锁不具备按 key 隔离的语义，不同 key 也必须在同一把锁上串行。

SingleFlight 正是针对这些问题的解决方案。

---

## 3. SingleFlight 工作原理

SingleFlight 在 `Group.load` 方法中包裹实际的数据加载逻辑（`loadData`）：

```text
请求 1 (key="user:123")
  └─ load() → loader.Do(key, loadData)
       │
       ├─ Load("user:123") → false（无已有 call）
       ├─ 创建 call，wg.Add(1)，Store("user:123", call)
       ├─ 执行 loadData（远端拉取 / Getter 回调）
       └─ wg.Done() → Delete("user:123")

请求 2~N (key="user:123") — 在请求 1 执行期间到达
  └─ load() → loader.Do(key, loadData)
       │
       └─ Load("user:123") → true（已有 call）
            └─ wg.Wait() → 阻塞
                 └─ 请求 1 完成后唤醒 → 共享 c.val, c.err
```

无论同一时刻有多少 goroutine 并发请求同一个 key，`loadData`（远端 gRPC 拉取或数据库查询）最多执行一次。

---

## 4. 核心数据结构

```go
type call struct {
    wg  sync.WaitGroup  // 阻塞后续请求，等待首个请求完成
    val interface{}     // 首个请求的执行结果
    err error           // 首个请求的执行错误
}

type Group struct {
    m sync.Map          // key → *call
}
```

**`call`** 代表一个正在进行或已完成的请求。`sync.WaitGroup` 作为"发布-订阅"机制：首个请求 `wg.Add(1)` → 执行 → `wg.Done()` 唤醒所有等待者。无需额外的 channel 或回调注册。

**`Group`** 使用 `sync.Map` 存储 key 到 `call` 的映射。选择 `sync.Map` 而非 `sync.RWMutex + map`，因为该场景"读多写少"——检查 key 是否已有 call（读）远多于创建/删除 call（写）。`sync.Map` 的读写分离设计（read map + dirty map 双 map 结构）恰好发挥优势：读操作优先从 read map 无锁读取。

---

## 5. Do 方法详解

```go
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error)
```

### 5.1 首个请求：创建 call 并执行

```go
// ① 同步点检查 — 无已有 call，不进入等待分支
if existing, ok := g.m.Load(key); ok {
    // ...
}

// ② 创建 call 并注册
c := &call{}
c.wg.Add(1)        // WaitGroup 计数器置 1，表示有一个请求进行中
g.m.Store(key, c)  // 注册到 map，后续请求可发现

// ③ defer 注册资源释放逻辑（详见 §5.3）

// ④ 执行实际数据加载
c.val, c.err = fn()
```

### 5.2 后续请求：等待并共享结果

```go
if existing, ok := g.m.Load(key); ok {
    c := existing.(*call)
    c.wg.Wait()         // 阻塞，等待首个请求完成
    return c.val, c.err // 直接共享结果，不执行 fn
}
```

`wg.Wait()` 使 goroutine 通过 `runtime_Semacquire` 进入休眠，不占用 CPU。首个请求调用 `wg.Done()` 后，所有等待者被同时唤醒，从 `c.val` / `c.err` 读取结果。

### 5.3 defer 保障的资源安全释放

```go
defer func() {
    if r := recover(); r != nil {
        // panic 路径：记录错误 → 唤醒等待者 → 清理 → 重新抛出
        c.err = fmt.Errorf("singleflight: panic recovered: %v", r)
        c.wg.Done()
        g.m.Delete(key)
        panic(r)
    }
    c.wg.Done()     // 正常路径：唤醒所有等待者
    g.m.Delete(key) // 清理 map，防止内存泄漏
}()
```

设计考量：

| 层面           | 说明                                                                                                                                             |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| **panic 安全** | 若 fn 内部 panic，裸写的 `wg.Done()` 永远不会执行 → 后续请求永久阻塞（goroutine 泄漏）。defer 保证即使 panic，等待者也会被唤醒并收到错误信息     |
| **代码健壮性** | 资源释放集中管理，避免多返回点遗漏清理                                                                                                           |
| **删除时机**   | `Delete(key)` 在 `wg.Done()` **之后**。若先删后唤醒，在窗口内新到达的请求发现 map 中无 call，会创建新 call 并再次执行 fn，破坏"只执行一次"的语义 |
| **重新抛出**   | `panic(r)` 让调用方感知异常，不静默吞掉 panic                                                                                                    |

---

## 6. 竞态窗口：乐观并发控制

`Load(key)` 返回 `false` 到 `Store(key, c)` 完成之间存在一个微小的竞态窗口：

```text
goroutine A: Load("x") → false ──── Store("x", callA)
goroutine B:        Load("x") → false ──── Store("x", callB)
                        ↑
                  两者同时通过 Load 检查，各自创建 call
```

在真实生产环境中，这个窗口极短（几纳秒），请求也非同时到达。即使发生，也仅导致 fn 多执行 1~2 次，不会有数据一致性问题。这是一种**乐观并发控制（Optimistic Concurrency Control）**的策略取舍——用极低概率的额外执行，换取无锁的快速路径。

> 单元测试中通过 channel 同步控制 goroutine 启动时机来规避此窗口，确保测试的确定性。实际代码不需要额外同步。

---

## 7. 在 ReachCache 中的定位

SingleFlight 位于 Group 协调层的 `load` 方法中，处于三级回源策略的关键位置：

```text
group.Get(key)
  │
  ├─ 本地缓存查询 → 命中则返回
  │
  └─ 未命中 → group.load(key)
                │
                └─ loader.Do(key, func() {  ← SingleFlight 在这里
                       loadData(key)
                         ├─ 远端节点拉取
                         └─ Getter 回调（数据库）
                   })
```

它从"垂直"维度防止同一 key 的并发回源——无论多少请求，`loadData` 只执行一次。配合一致性哈希从"水平"维度分散流量，构成 ReachCache 完整的高并发防护体系。

---

## 8. 单元测试覆盖

| 分类     | 测试                            | 覆盖点                                                                     |
| -------- | ------------------------------- | -------------------------------------------------------------------------- |
| 基本功能 | `TestDo_SingleCall`             | 单次调用返回正确结果                                                       |
|          | `TestDo_ErrorPropagation`       | fn 返回的错误正确传播                                                      |
| 并发合并 | `TestDo_ConcurrentSameKey`      | 首请求注册 call 后，后续 99 个全部命中 `Load` 走等待路径，fn 恰好执行 1 次 |
|          | `TestDo_DifferentKeys`          | 不同 key 各自独立执行 fn，互不阻塞                                         |
| 结果共享 | `TestDo_SharedResultCrossBatch` | 跨批次调用重新执行 fn（key 已完成并清理）                                  |
|          | `TestDo_ConcurrentSharedResult` | 同批次 50 goroutine 获得完全相同的返回值                                   |
| Panic    | `TestDo_PanicRecovery`          | fn panic 被正确重新抛出                                                    |
|          | `TestDo_PanicWakesWaiters`      | defer 保证 panic 后等待者不被永久阻塞                                      |
| Key 清理 | `TestDo_KeyCleanup`             | Do 完成后 key 从 map 中删除，不残留                                        |
| 边界     | `TestDo_EmptyKey`               | 空字符串 key 正常工作                                                      |
|          | `TestDo_NilResult`              | fn 返回 nil 正常处理                                                       |
| 混合     | `TestDo_MixedKeys`              | 5 个 key × 20 goroutine，每个 key 的 fn 恰好执行 1 次                      |

---

## 9. 使用示例

```go
package main

import (
    "fmt"
    "sync"

    "github.com/vernmorn/reachcache/singleflight"
)

func main() {
    var g singleflight.Group

    // 模拟从数据库加载
    loadFromDB := func(key string) (interface{}, error) {
        // 耗时操作...
        return "data-for-" + key, nil
    }

    // 10 个并发请求同一个 key
    var wg sync.WaitGroup
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            val, err := g.Do("user:123", func() (interface{}, error) {
                return loadFromDB("user:123")
            })
            fmt.Printf("goroutine %d: val=%v, err=%v\n", id, val, err)
        }(i)
    }
    wg.Wait()
    // loadFromDB 只被调用 1 次，10 个 goroutine 共享结果
}
```
