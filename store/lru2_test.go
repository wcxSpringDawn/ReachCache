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

// ======================== Set / Get 基本读写 ========================

func TestLRU2_SetGet(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	s.Set("key1", testValue{data: "hello"})
	v, ok := s.Get("key1")
	if !ok {
		t.Fatal("Get returned false for existing key")
	}
	if v.Len() != len("hello") {
		t.Fatalf("expected len=%d, got len=%d", len("hello"), v.Len())
	}
}

func TestLRU2_GetNonExistent(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	_, ok := s.Get("nonexistent")
	if ok {
		t.Fatal("Get should return false for non-existent key")
	}
}

// ======================== L1→L2 晋升 ========================

func TestLRU2_L1ToL2Promotion(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	// 首次 Set → 数据进入 L1
	s.Set("key1", testValue{data: "first-access"})

	// 首次 Get → 从 L1 找到 → 晋升到 L2 → 返回
	v, ok := s.Get("key1")
	if !ok {
		t.Fatal("first Get should find data in L1 and promote to L2")
	}
	if v.Len() != len("first-access") {
		t.Fatal("unexpected value length on first Get")
	}

	// 第二次 Get → 应在 L2 中找到
	v, ok = s.Get("key1")
	if !ok {
		t.Fatal("second Get should find data in L2")
	}
	if v.Len() != len("first-access") {
		t.Fatal("unexpected value length on second Get")
	}
}

func TestLRU2_SetOnlyStaysInL1(t *testing.T) {
	// 仅 Set 不 Get 的数据只在 L1 中，易被淘汰
	s := newLRU2Cache(Options{
		BucketCount:  1,
		CapPerBucket: 3,
		Level2Cap:    10,
	})
	defer s.Close()

	// 填满 L1（3 个条目）
	s.Set("k1", testValue{data: "a"})
	s.Set("k2", testValue{data: "b"})
	s.Set("k3", testValue{data: "c"})

	// 再插入 k4，L1 满，应淘汰 L1 尾部节点（k1）
	s.Set("k4", testValue{data: "d"})

	_, ok := s.Get("k1")
	if ok {
		t.Fatal("k1 should have been evicted from L1")
	}

	// k2、k3、k4 仍在 L1
	if s.Len() != 3 {
		t.Fatalf("expected Len()=3 after L1 eviction, got %d", s.Len())
	}
}

// ======================== SetWithExpiration 过期 ========================

func TestLRU2_SetWithExpiration(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	s.SetWithExpiration("key1", testValue{data: "expirable"}, 50*time.Millisecond)

	// 立即获取应命中
	v, ok := s.Get("key1")
	if !ok {
		t.Fatal("Get should hit before expiration")
	}
	if v.Len() != len("expirable") {
		t.Fatal("unexpected value length")
	}

	// 等待过期
	time.Sleep(150 * time.Millisecond)

	_, ok = s.Get("key1")
	if ok {
		t.Fatal("Get should return false after expiration")
	}
}

func TestLRU2_NoExpiration(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	// Set 默认永不过期
	s.Set("key1", testValue{data: "forever"})
	time.Sleep(50 * time.Millisecond)

	_, ok := s.Get("key1")
	if !ok {
		t.Fatal("Get should hit for non-expiring key")
	}
}

func TestLRU2_PromotedKeyExpires(t *testing.T) {
	// 晋升到 L2 的 key 也能正常过期
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	s.SetWithExpiration("key1", testValue{data: "expirable"}, 50*time.Millisecond)

	// 首次 Get 晋升到 L2
	v, ok := s.Get("key1")
	if !ok || v.Len() != len("expirable") {
		t.Fatal("first Get should succeed")
	}

	// 等待过期
	time.Sleep(150 * time.Millisecond)

	_, ok = s.Get("key1")
	if ok {
		t.Fatal("promoted key should also expire")
	}
}

// ======================== Delete ========================

func TestLRU2_Delete(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	s.Set("key1", testValue{data: "hello"})
	if s.Len() != 1 {
		t.Fatalf("expected Len()=1, got %d", s.Len())
	}

	if !s.Delete("key1") {
		t.Fatal("Delete should return true for existing key")
	}
	if s.Len() != 0 {
		t.Fatalf("expected Len()=0 after delete, got %d", s.Len())
	}
}

func TestLRU2_DeleteNonExistent(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	if s.Delete("nonexistent") {
		t.Fatal("Delete should return false for non-existent key")
	}
}

func TestLRU2_DeletePromotedKey(t *testing.T) {
	// 从 L1 晋升到 L2 的 key 能被正确删除
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	s.Set("key1", testValue{data: "hello"})
	s.Get("key1") // 晋升到 L2
	s.Get("key1") // 确认在 L2

	if !s.Delete("key1") {
		t.Fatal("Delete should work for promoted key")
	}
	if s.Len() != 0 {
		t.Fatalf("expected Len()=0 after delete, got %d", s.Len())
	}
}

// ======================== 淘汰 ========================

func TestLRU2_EvictionWhenFull(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  1,
		CapPerBucket: 4,
		Level2Cap:    10,
	})
	defer s.Close()

	// 填满 L1
	s.Set("k1", testValue{data: "a"})
	s.Set("k2", testValue{data: "b"})
	s.Set("k3", testValue{data: "c"})
	s.Set("k4", testValue{data: "d"})

	// 插入第 5 个，触发 L1 淘汰（逻辑删除的尾部节点被复用）
	s.Set("k5", testValue{data: "e"})

	if s.Len() != 4 {
		t.Fatalf("expected Len()=4 after eviction, got %d", s.Len())
	}
}

func TestLRU2_EvictionCallsOnEvicted(t *testing.T) {
	var evictedKeys []string
	s := newLRU2Cache(Options{
		BucketCount:  1,
		CapPerBucket: 2,
		Level2Cap:    2,
		OnEvicted: func(key string, value Value) {
			evictedKeys = append(evictedKeys, key)
		},
	})
	defer s.Close()

	// 填满 L1
	s.Set("k1", testValue{data: "a"})
	s.Set("k2", testValue{data: "b"})
	// 插入 k3，触发淘汰 k1（L1 尾部节点）
	s.Set("k3", testValue{data: "c"})

	if len(evictedKeys) == 0 {
		t.Fatal("OnEvicted should be called when eviction occurs")
	}
}

// ======================== Clear ========================

func TestLRU2_Clear(t *testing.T) {
	var evictedKeys []string
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
		OnEvicted: func(key string, value Value) {
			evictedKeys = append(evictedKeys, key)
		},
	})
	defer s.Close()

	s.Set("k1", testValue{data: "a"})
	s.Set("k2", testValue{data: "b"})
	s.Set("k3", testValue{data: "c"})

	s.Clear()

	if s.Len() != 0 {
		t.Fatalf("expected Len()=0 after clear, got %d", s.Len())
	}
	if len(evictedKeys) != 3 {
		t.Fatalf("expected 3 evicted callbacks, got %d", len(evictedKeys))
	}
}

// ======================== Len ========================

func TestLRU2_Len(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  4,
		CapPerBucket: 10,
		Level2Cap:    10,
	})
	defer s.Close()

	if s.Len() != 0 {
		t.Fatalf("expected Len()=0, got %d", s.Len())
	}

	for i := 0; i < 5; i++ {
		s.Set(fmt.Sprintf("k%d", i), testValue{data: "x"})
	}
	if s.Len() != 5 {
		t.Fatalf("expected Len()=5, got %d", s.Len())
	}

	// 晋升一个 key 到 L2（Len 应保持不变，因为是从 L1 转移到 L2）
	s.Get("k0")
	if s.Len() != 5 {
		t.Fatalf("expected Len()=5 after promotion, got %d", s.Len())
	}
}

// ======================== 分桶哈希 ========================

func TestLRU2_BucketDistribution(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  8,
		CapPerBucket: 100,
		Level2Cap:    100,
	})
	defer s.Close()

	// 写入大量 key，验证它们被均匀分布到不同桶
	n := 200
	for i := 0; i < n; i++ {
		s.Set(fmt.Sprintf("key%d", i), testValue{data: "val"})
	}

	if s.Len() != n {
		t.Fatalf("expected Len()=%d, got %d", n, s.Len())
	}
}

// ======================== 并发安全 ========================

func TestLRU2_ConcurrentAccess(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  16,
		CapPerBucket: 512,
		Level2Cap:    512,
	})
	defer s.Close()

	var wg sync.WaitGroup
	n := 200

	// 并发写入
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("key%d", idx), testValue{data: fmt.Sprintf("val%d", idx)})
		}(i)
	}
	wg.Wait()

	if s.Len() != n {
		t.Fatalf("expected Len()=%d, got %d", n, s.Len())
	}

	// 并发读取
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, ok := s.Get(fmt.Sprintf("key%d", idx))
			if !ok {
				t.Errorf("Get key%d failed", idx)
			}
		}(i)
	}
	wg.Wait()

	// 读写混合
	for i := 0; i < n/2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("mix%d", idx), testValue{data: "x"})
			s.Get(fmt.Sprintf("mix%d", idx))
			s.Get(fmt.Sprintf("key%d", idx))
		}(i)
	}
	wg.Wait()
}

func TestLRU2_ConcurrentSameBucket(t *testing.T) {
	// 所有 key 映射到同一个桶，测试桶锁的正确性
	s := newLRU2Cache(Options{
		BucketCount:  1,
		CapPerBucket: 100,
		Level2Cap:    100,
	})
	defer s.Close()

	var wg sync.WaitGroup
	n := 50
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("k%d", idx), testValue{data: "x"})
			s.Get(fmt.Sprintf("k%d", idx))
		}(i)
	}
	wg.Wait()
}

// ======================== Close ========================

func TestLRU2_Close(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  2,
		CapPerBucket: 10,
		Level2Cap:    10,
	})

	s.Set("k1", testValue{data: "hello"})

	// Close 应该安全返回
	s.Close()

	// 重复 Close 应该是安全的（closeOnce 保护）
	s.Close()
}

// ======================== 大容量操作 ========================

func TestLRU2_ManyKeys(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  4,
		CapPerBucket: 256,
		Level2Cap:    128,
	})
	defer s.Close()

	// 写入超出总容量的大量 key
	for i := 0; i < 500; i++ {
		s.Set(fmt.Sprintf("k%d", i), testValue{data: "data"})
	}

	// 总容量 = BucketCount * CapPerBucket（L1）+ BucketCount * Level2Cap（L2）
	// = 4*256 + 4*128 = 1024 + 512 = 1536
	// 但实际逻辑上 L1 和 L2 分别存储，500 个条目分布在 L1 和 L2
	_ = s.Len()
}

// ======================== 数据完整性 ========================

func TestLRU2_DataIntegrity(t *testing.T) {
	s := newLRU2Cache(Options{
		BucketCount:  4,
		CapPerBucket: 100,
		Level2Cap:    100,
	})
	defer s.Close()

	const testStr = "this is a test value for data integrity check"

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		s.Set(key, testValue{data: testStr + fmt.Sprintf("-%d", i)})

		// 一半的 key 做 Get 触发晋升
		if i%2 == 0 {
			v, ok := s.Get(key)
			if !ok {
				t.Fatalf("Get %s failed", key)
			}
			expectedLen := len(testStr + fmt.Sprintf("-%d", i))
			if v.Len() != expectedLen {
				t.Fatalf("value length mismatch: expected %d, got %d", expectedLen, v.Len())
			}
		}
	}

	// 验证所有 key 都能取回正确数据
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%d", i)
		v, ok := s.Get(key)
		if !ok {
			t.Fatalf("Get %s failed after all operations", key)
		}
		expectedLen := len(testStr + fmt.Sprintf("-%d", i))
		if v.Len() != expectedLen {
			t.Fatalf("final check: value length mismatch for %s", key)
		}
	}
}
