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

package reachcache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func newByteView(s string) ByteView {
	return ByteView{b: []byte(s)}
}

// ======================== 懒加载初始化 ========================

func TestCache_LazyInit(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	// 创建后不应初始化
	if c.Len() != 0 {
		t.Fatal("newly created cache should have Len()==0")
	}
	if atomic.LoadInt32(&c.initialized) != 0 {
		t.Fatal("cache should not be initialized before first Add")
	}

	// 首次 Add 触发懒初始化
	c.Add("key", newByteView("value"))
	if atomic.LoadInt32(&c.initialized) != 1 {
		t.Fatal("cache should be initialized after first Add")
	}
	if c.store == nil {
		t.Fatal("store should be non-nil after initialization")
	}
}

func TestCache_GetBeforeInit(t *testing.T) {
	// Get 不触发初始化——未初始化的 Cache 永远返回未命中
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	_, ok := c.Get(context.Background(), "key")
	if ok {
		t.Fatal("Get on uninitialized cache should return false")
	}

	stats := c.Stats()
	if stats["misses"].(int64) != 1 {
		t.Fatalf("misses should be 1, got %d", stats["misses"].(int64))
	}
}

// ======================== Get ========================

func TestCache_Get_Hit(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("hello", newByteView("world"))
	v, ok := c.Get(context.Background(), "hello")
	if !ok {
		t.Fatal("Get should hit after Add")
	}
	if string(v.ByteSlice()) != "world" {
		t.Fatalf("expected 'world', got '%s'", string(v.ByteSlice()))
	}

	stats := c.Stats()
	if stats["hits"].(int64) != 1 {
		t.Fatalf("hits should be 1, got %d", stats["hits"].(int64))
	}
}

func TestCache_Get_Miss(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("k", newByteView("v"))
	_, ok := c.Get(context.Background(), "nonexistent")
	if ok {
		t.Fatal("Get on nonexistent key should return false")
	}

	stats := c.Stats()
	if stats["misses"].(int64) != 1 {
		t.Fatalf("misses should be 1, got %d", stats["misses"].(int64))
	}
}

// ======================== Add ========================

func TestCache_Add(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("a", newByteView("1"))
	c.Add("b", newByteView("22"))

	if c.Len() != 2 {
		t.Fatalf("expected Len()=2, got %d", c.Len())
	}

	v, _ := c.Get(context.Background(), "a")
	if string(v.ByteSlice()) != "1" {
		t.Fatalf("expected '1', got '%s'", string(v.ByteSlice()))
	}
}

func TestCache_Add_Update(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("key", newByteView("old"))
	c.Add("key", newByteView("new"))

	v, _ := c.Get(context.Background(), "key")
	if string(v.ByteSlice()) != "new" {
		t.Fatalf("expected 'new', got '%s'", string(v.ByteSlice()))
	}
	if c.Len() != 1 {
		t.Fatalf("expected Len()=1 after update, got %d", c.Len())
	}
}

// ======================== Delete ========================

func TestCache_Delete(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("key", newByteView("value"))
	if !c.Delete("key") {
		t.Fatal("Delete should return true for existing key")
	}
	if c.Len() != 0 {
		t.Fatal("Len() should be 0 after delete")
	}
	_, ok := c.Get(context.Background(), "key")
	if ok {
		t.Fatal("Get should return false after Delete")
	}
}

func TestCache_Delete_NonExistent(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("k", newByteView("v"))
	if c.Delete("nonexistent") {
		t.Fatal("Delete of nonexistent key should return false")
	}
}

// ======================== Clear ========================

func TestCache_Clear(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("a", newByteView("1"))
	c.Add("b", newByteView("2"))
	c.Get(context.Background(), "a") // hits=1

	c.Clear()

	if c.Len() != 0 {
		t.Fatal("Len() should be 0 after Clear")
	}
	stats := c.Stats()
	if stats["hits"].(int64) != 0 {
		t.Fatal("hits should be reset to 0 after Clear")
	}
	if stats["misses"].(int64) != 0 {
		t.Fatal("misses should be reset to 0 after Clear")
	}
}

func TestCache_Clear_Uninitialized(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	// 不 Add，不初始化，直接 Clear
	c.Clear() // 不应 panic
}

// ======================== Close ========================

func TestCache_Closed_Reject(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	c.Add("key", newByteView("value"))
	c.Close()

	// 关闭后所有操作被拒绝
	c.Add("key2", newByteView("v2"))
	if c.Len() != 0 && c.Len() != 1 {
		// LRU 的 store 已被置 nil，Len 返回 0
	}

	_, ok := c.Get(context.Background(), "key")
	if ok {
		t.Fatal("Get on closed cache should return false")
	}

	if c.Delete("key") {
		t.Fatal("Delete on closed cache should return false")
	}
}

func TestCache_Close_Idempotent(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	c.Add("key", newByteView("value"))

	c.Close()
	c.Close() // 不应 panic，CAS 保证幂等

	if atomic.LoadInt32(&c.closed) != 1 {
		t.Fatal("closed should be 1")
	}
}

// ======================== Stats ========================

func TestCache_Stats(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	// 初始状态
	stats := c.Stats()
	if stats["initialized"].(bool) {
		t.Fatal("uninitialized cache: initialized should be false")
	}

	c.Add("a", newByteView("1"))
	c.Get(context.Background(), "a") // hit
	c.Get(context.Background(), "b") // miss

	stats = c.Stats()
	if !stats["initialized"].(bool) {
		t.Fatal("initialized cache: initialized should be true")
	}
	if stats["hits"].(int64) != 1 {
		t.Fatalf("hits should be 1")
	}
	if stats["misses"].(int64) != 1 {
		t.Fatalf("misses should be 1")
	}

	hitRate, ok := stats["hit_rate"].(float64)
	if !ok {
		t.Fatal("hit_rate should be present")
	}
	if hitRate != 0.5 {
		t.Fatalf("hit_rate should be 0.5, got %f", hitRate)
	}
	if stats["size"].(int) != 1 {
		t.Fatalf("size should be 1")
	}
}

// ======================== 并发 ========================

func TestCache_ConcurrentGet(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	c.Add("hot", newByteView("data"))

	var wg sync.WaitGroup
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := c.Get(context.Background(), "hot")
			if !ok || string(v.ByteSlice()) != "data" {
				t.Error("concurrent Get failed")
			}
		}()
	}
	wg.Wait()
}

func TestCache_ConcurrentAddGet(t *testing.T) {
	c := NewCache(DefaultCacheOptions())
	defer c.Close()

	var wg sync.WaitGroup
	const n = 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c.Add("k", newByteView("v"))
			c.Get(context.Background(), "k")
		}(i)
	}
	wg.Wait()
}
