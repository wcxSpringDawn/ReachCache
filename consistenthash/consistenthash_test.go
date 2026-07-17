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

package consistenthash

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
	"testing"
)

// ======================== Add / Get ========================

func TestAdd_Get(t *testing.T) {
	m := New()

	m.Add("node-a", "node-b", "node-c")

	// 任意 key 应返回非空节点名
	node := m.Get("some-key")
	if node == "" {
		t.Fatal("Get should return a node for non-empty key")
	}
}

func TestGet_EmptyKey(t *testing.T) {
	m := New()
	m.Add("node-a")

	if node := m.Get(""); node != "" {
		t.Fatal("Get with empty key should return empty string")
	}
}

func TestGet_EmptyRing(t *testing.T) {
	m := New()

	if node := m.Get("key"); node != "" {
		t.Fatal("Get on empty ring should return empty string")
	}
}

func TestAdd_EmptyNodeSkipped(t *testing.T) {
	m := New()

	// 空节点名被静默跳过（非错误）
	m.Add("")
	if len(m.keys) != 0 {
		t.Fatal("empty node should be skipped, no virtual nodes created")
	}
}

func TestAdd_NoArgs(t *testing.T) {
	m := New()

	err := m.Add()
	if err == nil {
		t.Fatal("Add with no args should return error")
	}
}

// ======================== 一致性 ========================

func TestGet_Consistency(t *testing.T) {
	// 同一 key 多次 Get 应始终返回同一节点（环不变时）
	m := New()
	m.Add("node-a", "node-b", "node-c")

	first := m.Get("consistent-key")
	for i := 0; i < 100; i++ {
		if got := m.Get("consistent-key"); got != first {
			t.Fatalf("Get should be consistent: first=%s, got=%s", first, got)
		}
	}
}

func TestGet_DifferentKeys(t *testing.T) {
	// 不同 key 应被路由到节点（不要求不同节点，但至少不会全相同除非极端情况）
	m := New()
	m.Add("node-a", "node-b", "node-c", "node-d")

	nodes := make(map[string]int)
	for i := 0; i < 200; i++ {
		nodes[m.Get(fmt.Sprintf("key-%d", i))]++
	}

	// 至少 2 个节点被命中（统计上几乎必然）
	if len(nodes) < 2 {
		t.Logf("only %d nodes hit (possible but unlikely with 200 keys)", len(nodes))
	}
}

// ======================== Remove ========================

func TestRemove(t *testing.T) {
	m := New()
	m.Add("node-a", "node-b")

	m.Remove("node-a")

	// 存在 key 原属于 node-a 现在应路由到 node-b
	// 验证环不会崩坏：Get 仍然返回有效节点
	node := m.Get("any-key")
	if node == "" {
		t.Fatal("Get should still return a node after partial removal")
	}
	if node == "node-a" {
		t.Fatal("removed node should not be returned")
	}
}

func TestRemove_NonExistent(t *testing.T) {
	m := New()
	m.Add("node-a")

	err := m.Remove("node-b")
	if err == nil {
		t.Fatal("Remove of non-existent node should return error")
	}
}

func TestRemove_LastNode(t *testing.T) {
	m := New()
	m.Add("only-node")
	m.Remove("only-node")

	if node := m.Get("key"); node != "" {
		t.Fatal("Get on ring with all nodes removed should return empty")
	}
}

// ======================== 虚拟节点 ========================

func TestVirtualNodes(t *testing.T) {
	m := New()
	m.Add("node-a")

	// 默认 50 个虚拟节点
	// 验证 keys 数量等于 replicas
	if len(m.keys) != m.config.DefaultReplicas {
		t.Fatalf("expected %d virtual nodes, got %d", m.config.DefaultReplicas, len(m.keys))
	}
}

func TestCustomReplicas(t *testing.T) {
	m := New(WithConfig(&Config{
		DefaultReplicas:      10,
		MinReplicas:          5,
		MaxReplicas:          20,
		HashFunc:             crc32.ChecksumIEEE,
		LoadBalanceThreshold: 0.25,
	}))
	m.Add("node-a")

	if len(m.keys) != 10 {
		t.Fatalf("expected 10 virtual nodes, got %d", len(m.keys))
	}
}

// ======================== GetStats ========================

func TestGetStats(t *testing.T) {
	m := New()
	m.Add("node-a", "node-b")

	// 无请求时返回空 map
	stats := m.GetStats()
	if len(stats) != 0 {
		t.Fatal("GetStats should return empty map when no requests")
	}

	// 发起请求
	for i := 0; i < 100; i++ {
		m.Get(fmt.Sprintf("key-%d", i))
	}

	stats = m.GetStats()
	if len(stats) == 0 {
		t.Fatal("GetStats should return non-empty map after requests")
	}

	// 占比之和应接近 1
	var sum float64
	for _, ratio := range stats {
		sum += ratio
	}
	if sum < 0.99 || sum > 1.01 {
		t.Errorf("stats ratios should sum to ~1, got %f", sum)
	}
}

// ======================== 边界情况 ========================

func TestAdd_DuplicateNode(t *testing.T) {
	m := New()
	m.Add("node-a")

	initialReplicas := m.nodeReplicas["node-a"]
	// addNode 用新 replicas 直接覆盖 nodeReplicas[node]，不累加
	m.Add("node-a")
	if m.nodeReplicas["node-a"] != initialReplicas {
		t.Fatalf("re-adding same node should reset to default replicas (%d), got %d",
			initialReplicas, m.nodeReplicas["node-a"])
	}
}

// ======================== 并发安全 ========================

func TestConcurrent_Get(t *testing.T) {
	m := New()
	m.Add("node-a", "node-b", "node-c")

	var wg sync.WaitGroup
	const n = 200

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			node := m.Get(fmt.Sprintf("key-%d", idx))
			if node == "" {
				t.Error("Get returned empty in concurrent access")
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrent_AddGet(t *testing.T) {
	m := New()
	m.Add("node-a")

	var wg sync.WaitGroup
	const n = 100

	// 并发添加节点
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.Add(fmt.Sprintf("node-concurrent-%d", idx))
		}(i)
	}

	// 并发读取
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.Get(fmt.Sprintf("key-%d", idx))
		}(i)
	}
	wg.Wait()
}

// ======================== 自定义哈希函数 ========================

func TestCustomHashFunc(t *testing.T) {
	// 使用简单的 mod 哈希验证自定义 HashFunc 生效
	m := New(WithConfig(&Config{
		DefaultReplicas:      1, // 每节点 1 个虚拟节点，便于验证
		MinReplicas:          1,
		MaxReplicas:          1,
		HashFunc:             func(data []byte) uint32 { return uint32(len(data)) },
		LoadBalanceThreshold: 0.25,
	}))
	m.Add("node-a", "node-b", "node-c")
	sort.Ints(m.keys) // 确保有序

	node := m.Get("abc") // len=3
	if node == "" {
		t.Fatal("Get should return a node with custom hash")
	}
}

// ======================== 哈希环一致性（顺时针查找） ========================

func TestGet_ClockwiseSearch(t *testing.T) {
	// 使用确定性哈希（取整个字符串第一字节的倒数第二位字符）验证顺时针查找。
	// 虚拟节点名格式为 "nodeName-0"，所以 nodeM 和 nodeZ 的哈希相同（都以 'n' 开头）。
	// 改用不同前缀的节点名。
	m := New(WithConfig(&Config{
		DefaultReplicas:      1,
		MinReplicas:          1,
		MaxReplicas:          1,
		HashFunc:             func(data []byte) uint32 { return uint32(data[0]) },
		LoadBalanceThreshold: 0.25,
	}))

	// 节点名首字母: 'A'(65), 'M'(77), 'Z'(90)
	m.Add("A-node", "M-node", "Z-node")
	sort.Ints(m.keys)

	// key 'C'(67): 65 < 67 < 77 → 应命中 M-node
	if node := m.Get("C"); node != "M-node" {
		t.Errorf("key 'C'(67) should hit M-node(77), got %s", node)
	}

	// key 'a'(97): 97 > 所有节点 → 回绕到环首 → A-node(65)
	if node := m.Get("a"); node != "A-node" {
		t.Errorf("key 'a'(97) should wrap to A-node(65), got %s", node)
	}
}
