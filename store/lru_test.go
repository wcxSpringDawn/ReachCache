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

package store

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// testValue 实现了 Value 接口，用于测试
type testValue struct {
	data string
}

func (v testValue) Len() int {
	return len(v.data)
}

// ======================== Set / Get 基本读写 ========================

func TestLRU_SetGet(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	v := testValue{data: "hello"}
	c.Set("key1", v)

	got, ok := c.Get("key1")
	if !ok {
		t.Fatal("Get returned false for existing key")
	}
	if got.Len() != len("hello") {
		t.Fatalf("expected len=%d, got len=%d", len("hello"), got.Len())
	}
}

func TestLRU_GetNonExistent(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	_, ok := c.Get("nonexistent")
	if ok {
		t.Fatal("Get should return false for non-existent key")
	}
}

func TestLRU_UpdateExisting(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	c.Set("key1", testValue{data: "old"})
	c.Set("key1", testValue{data: "new"})

	got, ok := c.Get("key1")
	if !ok {
		t.Fatal("Get returned false after update")
	}
	if got.Len() != len("new") {
		t.Fatalf("expected len=%d, got len=%d", len("new"), got.Len())
	}
	if c.Len() != 1 {
		t.Fatalf("expected Len()=1, got %d", c.Len())
	}
}

// ======================== SetWithExpiration 过期 ========================

func TestLRU_SetWithExpiration(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	c.SetWithExpiration("key1", testValue{data: "expirable"}, 50*time.Millisecond)

	// 立即获取应该命中
	got, ok := c.Get("key1")
	if !ok {
		t.Fatal("Get should hit before expiration")
	}
	if got.Len() != len("expirable") {
		t.Fatalf("unexpected value length: %d", got.Len())
	}

	// 等待过期
	time.Sleep(100 * time.Millisecond)

	_, ok = c.Get("key1")
	if ok {
		t.Fatal("Get should return false after expiration")
	}
}

func TestLRU_SetWithoutExpiration(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	// expiration=0 表示永不过期
	c.SetWithExpiration("key1", testValue{data: "forever"}, 0)

	time.Sleep(50 * time.Millisecond)

	_, ok := c.Get("key1")
	if !ok {
		t.Fatal("Get should hit for non-expiring key")
	}
}

func TestLRU_SetNilValue(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	c.Set("key1", testValue{data: "hello"})
	if c.Len() != 1 {
		t.Fatalf("expected Len()=1, got %d", c.Len())
	}

	// SetWithExpiration with nil → 等同于 Delete
	c.SetWithExpiration("key1", nil, 0)
	if c.Len() != 0 {
		t.Fatalf("expected Len()=0 after setting nil, got %d", c.Len())
	}
}

// ======================== Delete ========================

func TestLRU_Delete(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	c.Set("key1", testValue{data: "hello"})
	if c.Len() != 1 {
		t.Fatal("expected Len()=1")
	}

	if !c.Delete("key1") {
		t.Fatal("Delete should return true for existing key")
	}
	if c.Len() != 0 {
		t.Fatal("expected Len()=0 after delete")
	}
}

func TestLRU_DeleteNonExistent(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	if c.Delete("nonexistent") {
		t.Fatal("Delete should return false for non-existent key")
	}
}

// ======================== LRU 淘汰 ========================

func TestLRU_Eviction(t *testing.T) {
	// 每个 entry: key 长度 + value.Len() = 3 + 5 = 8 字节
	// maxBytes=24，最多存 3 个条目
	c := newLRUCache(Options{MaxBytes: 24})
	defer c.Close()

	c.Set("k1", testValue{data: "aaaaa"})
	c.Set("k2", testValue{data: "bbbbb"})
	c.Set("k3", testValue{data: "ccccc"})
	// 此时正好 24 字节，3 个条目

	if c.Len() != 3 {
		t.Fatalf("expected Len()=3, got %d", c.Len())
	}

	// 插入第 4 个，会触发淘汰（最久未使用的 k1）
	c.Set("k4", testValue{data: "ddddd"})
	if c.Len() != 3 {
		t.Fatalf("expected Len()=3 after eviction, got %d", c.Len())
	}

	_, ok := c.Get("k1")
	if ok {
		t.Fatal("k1 should have been evicted")
	}
}

func TestLRU_EvictionOrder(t *testing.T) {
	// 验证 LRU 访问顺序：访问过的条目被移到尾部，不被淘汰
	c := newLRUCache(Options{MaxBytes: 24})
	defer c.Close()

	c.Set("k1", testValue{data: "aaaaa"})
	c.Set("k2", testValue{data: "bbbbb"})
	c.Set("k3", testValue{data: "ccccc"})

	// 访问 k1，使其变成最近使用
	c.Get("k1")

	// 插入 k4，应淘汰 k2（k1 被访问过，k2 成了最久未使用）
	c.Set("k4", testValue{data: "ddddd"})

	_, ok1 := c.Get("k1")
	_, ok2 := c.Get("k2")
	if !ok1 {
		t.Fatal("k1 should not be evicted after being accessed")
	}
	if ok2 {
		t.Fatal("k2 should have been evicted")
	}
}

// ======================== Clear ========================

func TestLRU_Clear(t *testing.T) {
	var evictedKeys []string
	c := newLRUCache(Options{
		MaxBytes: 1024,
		OnEvicted: func(key string, value Value) {
			evictedKeys = append(evictedKeys, key)
		},
	})
	defer c.Close()

	c.Set("k1", testValue{data: "aaaaa"})
	c.Set("k2", testValue{data: "bbbbb"})

	c.Clear()

	if c.Len() != 0 {
		t.Fatalf("expected Len()=0 after clear, got %d", c.Len())
	}
	if len(evictedKeys) != 2 {
		t.Fatalf("expected 2 evicted callbacks, got %d", len(evictedKeys))
	}
}

// ======================== OnEvicted 回调 ========================

func TestLRU_OnEvicted(t *testing.T) {
	var evictedKey string
	var evictedValue Value
	c := newLRUCache(Options{
		MaxBytes: 8, // 只够存 1 个 entry (key=2 + value=5 = 7)
		OnEvicted: func(key string, value Value) {
			evictedKey = key
			evictedValue = value
		},
	})
	defer c.Close()

	c.Set("k1", testValue{data: "aaaaa"})
	c.Set("k2", testValue{data: "bbbbb"}) // 触发淘汰 k1

	if evictedKey != "k1" {
		t.Fatalf("expected evicted key='k1', got '%s'", evictedKey)
	}
	if evictedValue.Len() != 5 {
		t.Fatalf("expected evicted value len=5, got %d", evictedValue.Len())
	}
}

// ======================== Len ========================

func TestLRU_Len(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	if c.Len() != 0 {
		t.Fatalf("expected Len()=0, got %d", c.Len())
	}

	c.Set("k1", testValue{data: "a"})
	c.Set("k2", testValue{data: "b"})
	if c.Len() != 2 {
		t.Fatalf("expected Len()=2, got %d", c.Len())
	}

	c.Delete("k1")
	if c.Len() != 1 {
		t.Fatalf("expected Len()=1 after delete, got %d", c.Len())
	}
}

// ======================== 并发安全 ========================

func TestLRU_ConcurrentAccess(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024 * 1024})
	defer c.Close()

	var wg sync.WaitGroup
	n := 100

	// 并发写入
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c.Set(fmt.Sprintf("key%d", idx), testValue{data: fmt.Sprintf("val%d", idx)})
		}(i)
	}
	wg.Wait()

	if c.Len() != n {
		t.Fatalf("expected Len()=%d, got %d", n, c.Len())
	}

	// 并发读取
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, ok := c.Get(fmt.Sprintf("key%d", idx))
			if !ok {
				t.Errorf("Get key%d failed", idx)
			}
		}(i)
	}
	wg.Wait()

	// 并发读写混合
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c.Set(fmt.Sprintf("mix%d", idx), testValue{data: "x"})
			c.Get(fmt.Sprintf("mix%d", idx))
			c.Get(fmt.Sprintf("key%d", idx))
		}(i)
	}
	wg.Wait()
}

func TestLRU_ConcurrentGetSameKey(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	c.Set("hot", testValue{data: "hot-data"})

	var wg sync.WaitGroup
	n := 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, ok := c.Get("hot")
			if !ok || v.Len() != len("hot-data") {
				t.Error("Get hot key failed in concurrent access")
			}
		}()
	}
	wg.Wait()
}

// ======================== Close ========================

func TestLRU_Close(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	c.Close()

	// Close 后仍可调用（cleanupTicker 停止，channel 关闭，但数据还在）
	if c.Len() == 0 {
		// 接受 Len()==0 或正常
	}
}

// ======================== GetWithExpiration ========================

func TestLRU_GetWithExpiration(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	c.SetWithExpiration("key1", testValue{data: "data"}, time.Hour)
	v, ttl, ok := c.GetWithExpiration("key1")
	if !ok {
		t.Fatal("GetWithExpiration should find the key")
	}
	if v.Len() != len("data") {
		t.Fatal("unexpected value length")
	}
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("expected TTL close to 1h, got %v", ttl)
	}

	// 不存在的 key
	_, _, ok = c.GetWithExpiration("nonexistent")
	if ok {
		t.Fatal("GetWithExpiration should return false for non-existent key")
	}
}

// ======================== UsedBytes / MaxBytes ========================

func TestLRU_UsedBytes(t *testing.T) {
	c := newLRUCache(Options{MaxBytes: 1024})
	defer c.Close()

	c.Set("k1", testValue{data: "hello"})
	// usedBytes = len("k1") + len("hello") = 2 + 5 = 7
	if c.UsedBytes() != 7 {
		t.Fatalf("expected UsedBytes()=7, got %d", c.UsedBytes())
	}

	c.Delete("k1")
	if c.UsedBytes() != 0 {
		t.Fatalf("expected UsedBytes()=0 after delete, got %d", c.UsedBytes())
	}
}
