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

// Package consistenthash 实现了一致性哈希路由算法，支持动态节点增删和自适应负载均衡。
//
// 一致性哈希将整个哈希值空间组织成一个首尾相接的虚拟环。每个节点通过哈希函数在环上占据
// 多个位置（虚拟节点）。确定 key 归属时，计算 key 的哈希值，顺时针查找第一个 ≥ 该值的节点。
//
// 当节点增减时，仅影响约 1/N 的 key（N 为节点数），远优于取模哈希的 100%，从根本上防止
// 了节点变更引发的大规模缓存失效（缓存雪崩）。
//
// 为解决请求倾斜（热点 key 导致某些节点负载远高于其他），系统每秒检查各节点请求水位，
// 动态调整虚拟节点配比：过载节点减少副本、空闲节点增加副本。

package consistenthash

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Map 是一致性哈希环的核心结构，线程安全。
//
// 索引约定：
//   - keys 存储所有虚拟节点的哈希值（有序），支持二分查找
//   - hashMap 将虚拟节点的哈希值映射回真实节点名
//   - nodeCounts 使用 *int64 指针，绕过 Go map value 不可寻址的限制，支持原子操作
type Map struct {
	mu            sync.RWMutex
	config        *Config
	keys          []int             // 虚拟节点哈希值，已排序
	hashMap       map[int]string    // 哈希值 → 真实节点名
	nodeReplicas  map[string]int    // 真实节点名 → 虚拟节点数量
	nodeCounts    map[string]*int64 // 真实节点名 → 请求计数（*int64 支持原子操作）
	totalRequests int64             // 总请求数
	done          chan struct{}     // 关闭信号，停止负载均衡 goroutine
}

// New 创建一致性哈希实例并启动后台负载均衡器（每秒检查一次）。
// 可通过 WithConfig(...) 传入自定义配置。
func New(opts ...Option) *Map {
	m := &Map{
		config:       DefaultConfig,
		hashMap:      make(map[int]string),
		nodeReplicas: make(map[string]int),
		nodeCounts:   make(map[string]*int64),
		done:         make(chan struct{}),
	}

	for _, opt := range opts {
		opt(m)
	}

	m.startBalancer() // 启动负载均衡器
	return m
}

// Option 是 Map 的函数选项，用于自定义配置。
type Option func(*Map)

// WithConfig 使用自定义 Config 替换默认配置。
func WithConfig(config *Config) Option {
	return func(m *Map) {
		m.config = config
	}
}

// Add 将一个或多个节点加入哈希环。每个节点创建 DefaultReplicas 个虚拟节点，
// 虚拟节点格式为 "nodeName-i"（i 从 0 到 replicas-1）。空节点名被跳过。
func (m *Map) Add(nodes ...string) error {
	if len(nodes) == 0 {
		return errors.New("no nodes provided")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, node := range nodes {
		if node == "" {
			continue
		}

		// 为节点添加虚拟节点
		m.addNode(node, m.config.DefaultReplicas)
	}

	// 重新排序
	sort.Ints(m.keys)
	return nil
}

// Remove 从哈希环中移除指定节点及其所有虚拟节点。节点不存在时返回 error。
func (m *Map) Remove(node string) error {
	if node == "" {
		return errors.New("invalid node")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	replicas := m.nodeReplicas[node]
	if replicas == 0 {
		return fmt.Errorf("node %s not found", node)
	}

	// 移除节点的所有虚拟节点
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc(fmt.Appendf(nil, "%s-%d", node, i)))
		delete(m.hashMap, hash)
		for j := 0; j < len(m.keys); j++ {
			if m.keys[j] == hash {
				m.keys = append(m.keys[:j], m.keys[j+1:]...)
				break
			}
		}
	}

	delete(m.nodeReplicas, node)
	delete(m.nodeCounts, node)
	return nil
}

// Get 根据 key 查找其归属节点。
//
// 查找流程（O(log N)，N 为虚拟节点总数）：
//  1. 使用 HashFunc 计算 key 的哈希值
//  2. sort.Search 二分查找第一个 ≥ hash 的虚拟节点位置
//  3. 若 hash 大于所有虚拟节点，回绕到环首（idx=0）
//  4. 通过 hashMap 找到对应的真实节点
//  5. 原子递增该节点的请求计数（用于负载均衡采样）
//
// 返回 "" 表示 key 为空或哈希环无节点。
func (m *Map) Get(key string) string {
	if key == "" {
		return ""
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.config.HashFunc([]byte(key)))
	// 二分查找
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// 处理边界情况
	if idx == len(m.keys) {
		idx = 0
	}

	node := m.hashMap[m.keys[idx]]
	atomic.AddInt64(m.nodeCounts[node], 1)
	atomic.AddInt64(&m.totalRequests, 1)

	return node
}

// addNode 为指定节点创建 replicas 个虚拟节点并加入哈希环。
// 虚拟节点命名格式为 "nodeName-i"，哈希值存入 keys 切片，映射关系存入 hashMap。
// 调用前必须持有写锁，调用后需外部 sort.Ints(m.keys)。
func (m *Map) addNode(node string, replicas int) {
	for i := 0; i < replicas; i++ {
		hash := int(m.config.HashFunc(fmt.Appendf(nil, "%s-%d", node, i)))
		m.keys = append(m.keys, hash)
		m.hashMap[hash] = node
	}
	m.nodeReplicas[node] = replicas
	if _, ok := m.nodeCounts[node]; !ok {
		m.nodeCounts[node] = new(int64) // 分配零值 *int64
	}
}

// checkAndRebalance 检查负载不均衡度并在超过阈值时触发重平衡。
//
// 触发条件（两个条件同时满足）：
//   - 总请求数 ≥ 1000（避免小样本噪声导致误调整）
//   - 存在节点的 |负载 − 平均负载| / 平均负载 > LoadBalanceThreshold
//
// 计算负载分布时持有读锁保护 nodeReplicas 和 nodeCounts 的并发安全，
// 读取完毕后释放读锁再决定是否调用 rebalanceNodes()，防止 RWMutex 锁升级死锁。
//
// 由后台 goroutine 每秒调用一次。
func (m *Map) checkAndRebalance() {
	if atomic.LoadInt64(&m.totalRequests) < 1000 {
		return // 样本太少，不进行调整
	}

	m.mu.RLock()
	if len(m.nodeReplicas) == 0 {
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
		m.rebalanceNodes()
	}
}

// rebalanceNodes 执行虚拟节点重平衡。
//
// 调整策略：
//   - 过载节点（loadRatio > 1）：newReplicas = current / loadRatio（等比缩减）
//   - 空闲节点（loadRatio < 1）：newReplicas = current × (2 − loadRatio)（非对称增加）
//   - 调整后限制在 [MinReplicas, MaxReplicas] 范围内
//
// 过载节点使用除法（更积极恢复），空闲节点使用 2−ratio（防止过度调整）。
// 完成后重置所有计数器并重排 keys，进入下一个统计周期。
func (m *Map) rebalanceNodes() {
	m.mu.Lock()
	defer m.mu.Unlock()

	avgLoad := float64(m.totalRequests) / float64(len(m.nodeReplicas))

	// 调整每个节点的虚拟节点数量
	for node, cnt := range m.nodeCounts {
		currentReplicas := m.nodeReplicas[node]
		loadRatio := float64(atomic.LoadInt64(cnt)) / avgLoad

		var newReplicas int
		if loadRatio > 1 {
			// 负载过高，减少虚拟节点
			newReplicas = int(float64(currentReplicas) / loadRatio)
		} else {
			// 负载过低，增加虚拟节点
			newReplicas = int(float64(currentReplicas) * (2 - loadRatio))
		}

		// 确保在限制范围内
		if newReplicas < m.config.MinReplicas {
			newReplicas = m.config.MinReplicas
		}
		if newReplicas > m.config.MaxReplicas {
			newReplicas = m.config.MaxReplicas
		}

		if newReplicas != currentReplicas {
			// 重新添加节点的虚拟节点
			if err := m.Remove(node); err != nil {
				continue // 如果移除失败，跳过这个节点
			}
			m.addNode(node, newReplicas)
		}
	}

	// 重置计数器
	for _, cnt := range m.nodeCounts {
		atomic.StoreInt64(cnt, 0)
	}
	atomic.StoreInt64(&m.totalRequests, 0)

	// 重新排序
	sort.Ints(m.keys)
}

// GetStats 返回各节点的请求占比（0~1），用于监控负载分布。
// 总请求数为 0 时返回空 map。
func (m *Map) GetStats() map[string]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]float64)
	total := atomic.LoadInt64(&m.totalRequests)
	if total == 0 {
		return stats
	}

	for node, cnt := range m.nodeCounts {
		stats[node] = float64(atomic.LoadInt64(cnt)) / float64(total)
	}
	return stats
}

// startBalancer 启动后台负载均衡 goroutine，每秒调用 checkAndRebalance。
// 监听 done channel，Close() 时退出。
func (m *Map) startBalancer() {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.checkAndRebalance()
			case <-m.done:
				return
			}
		}
	}()
}

// Close 关闭 Map，停止后台负载均衡 goroutine。关闭后 Map 不可再使用。
func (m *Map) Close() {
	close(m.done)
}
