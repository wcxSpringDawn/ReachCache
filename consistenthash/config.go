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
// 核心设计：
//   - 虚拟节点机制：每个真实节点在哈希环上对应多个虚拟节点，解决数据倾斜
//   - 二分查找路由：基于 sort.Search 实现 O(log N) 的 key→node 查找
//   - 动态负载均衡：每秒检查各节点请求水位，过载节点减少虚拟节点、空闲节点增加虚拟节点
//
// 在 ReachCache 中，Map 被 ClientPicker 使用，为 Get/Set/Delete 操作提供 key 到缓存节点的路由决策。
package consistenthash

import "hash/crc32"

// Config 是一致性哈希的配置，控制虚拟节点数量范围和负载均衡行为。
type Config struct {
	// DefaultReplicas 每个真实节点加入时默认创建的虚拟节点数。
	DefaultReplicas int

	// MinReplicas 动态负载均衡时虚拟节点数量的下限。
	MinReplicas int

	// MaxReplicas 动态负载均衡时虚拟节点数量的上限。
	MaxReplicas int

	// HashFunc 哈希函数，用于将节点名和 key 映射到哈希环上。
	// 默认使用 crc32.ChecksumIEEE，硬件加速，分布均匀。
	HashFunc func(data []byte) uint32

	// LoadBalanceThreshold 负载不均衡度阈值（0~1）。
	// 当任一节点的不均衡度超过此值时触发虚拟节点重平衡。默认为 0.25（25%）。
	LoadBalanceThreshold float64
}

// DefaultConfig 返回推荐的默认配置：
//   - 每节点 50 个虚拟节点
//   - 动态调整范围 10~200
//   - CRC32-IEEE 哈希函数
//   - 25% 不均衡度阈值
var DefaultConfig = &Config{
	DefaultReplicas:      50,
	MinReplicas:          10,
	MaxReplicas:          200,
	HashFunc:             crc32.ChecksumIEEE,
	LoadBalanceThreshold: 0.25,
}
