/*
Copyright 2012 Google Inc.
Modification Copyright 2026 wcxSpringDawn

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

// Package singleflight 提供请求合并（SingleFlight）机制，用于防止缓存击穿。
//
// 核心原理：对于同一 key 的多个并发请求，仅让第一个实际执行数据加载，其余请求
// 阻塞等待并共享其结果。这保证了无论有多少并发请求，对后端数据源的"昂贵操作"
// 只执行一次。
//
// 在 ReachCache 中，SingleFlight 位于 Group.load 方法中，包裹 loadData（远端
// 拉取或 Getter 回调），是三级回源策略中防止并发重复加载的关键防线。
package singleflight

import (
	"fmt"
	"sync"
)

// call 代表一个正在进行或已完成的请求。
// 首个请求通过 wg.Add(1) 标记进行中，完成后 wg.Done() 唤醒所有等待者；
// 后续请求通过 wg.Wait() 阻塞并直接共享 val/err。
type call struct {
	wg  sync.WaitGroup // 其它请求可以在WaitGroup上等待这个请求完成
	val interface{}    // 存储请求的结果
	err error
}

// Group 管理所有正在进行中的请求，是 SingleFlight 的核心结构。
// 使用 sync.Map 而非 RWMutex+map，因为该场景"读多写少"（检查 key 是否已有 call
// 远多于创建/删除 call），sync.Map 的读写分离设计恰好发挥优势。
type Group struct {
	m sync.Map // key → *call
}

// Do 针对相同的 key，保证并发调用时 fn 只执行一次。
//
// 行为：
//   - 首次调用：创建 call，执行 fn，结果共享给后续等待者
//   - 后续调用：发现已有 call → wg.Wait() 阻塞 → 共享结果
//   - 不同 key：各自独立，互不阻塞
//
// Panic 恢复：若 fn 发生 panic，defer 中 recover 会：
//  1. 将错误信息写入 c.err，唤醒所有等待者（避免永久阻塞）
//  2. 从 map 中清理该 key
//  3. 重新抛出 panic（让调用方感知异常）
//
// 资源释放：无论正常返回还是 panic，defer 保证 wg.Done() 和 Delete(key) 必定执行，
// 防止 goroutine 泄漏和内存泄漏。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	// 已有相同 key 的 call 在进行中 → 等待并共享结果
	if existing, ok := g.m.Load(key); ok {
		c := existing.(*call)
		c.wg.Wait()
		return c.val, c.err
	}

	// 创建新 call 并注册到 map
	c := &call{}
	c.wg.Add(1)
	g.m.Store(key, c)

	// 使用defer确保无论是正常返回还是fn中发生panic，wg.Done()和Delete(key)都会执行，
	// 防止因panic导致后续请求永久阻塞在wg.Wait()上。
	// 注：这里的fn中发生panic是指我们无法确保用户注册的回源函数不会panic，所以我们需要在这里捕获panic，避免影响整个服务的稳定性。
	defer func() {
		if r := recover(); r != nil {
			c.err = fmt.Errorf("singleflight: panic recovered: %v", r)
			c.wg.Done()
			g.m.Delete(key)
			panic(r) // 重新抛出，让调用方感知
		}
		c.wg.Done()
		g.m.Delete(key)
	}()

	// 执行fn函数来获取数据，并将结果存储在call中
	c.val, c.err = fn()

	return c.val, c.err
}
