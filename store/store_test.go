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
	"testing"
	"time"
)

// ======================== NewStore 工厂函数 ========================

func TestNewStore_LRU(t *testing.T) {
	s := NewStore(LRU, NewOptions())
	defer s.Close()

	if s == nil {
		t.Fatal("NewStore(LRU) returned nil")
	}

	// 验证是 *lruCache 类型
	_, ok := s.(*lruCache)
	if !ok {
		t.Fatal("NewStore(LRU) should return *lruCache")
	}
}

func TestNewStore_LRU2(t *testing.T) {
	s := NewStore(LRU2, NewOptions())
	defer s.Close()

	if s == nil {
		t.Fatal("NewStore(LRU2) returned nil")
	}

	// 验证是 *lru2Store 类型
	_, ok := s.(*lru2Store)
	if !ok {
		t.Fatal("NewStore(LRU2) should return *lru2Store")
	}
}

func TestNewStore_Default(t *testing.T) {
	// 未知类型应默认为 LRU
	s := NewStore(CacheType("unknown"), NewOptions())
	defer s.Close()

	_, ok := s.(*lruCache)
	if !ok {
		t.Fatal("NewStore(unknown) should default to *lruCache")
	}
}

// ======================== NewOptions 默认值 ========================

func TestNewOptions_Defaults(t *testing.T) {
	opts := NewOptions()

	if opts.MaxBytes != 8192 {
		t.Fatalf("expected MaxBytes=8192, got %d", opts.MaxBytes)
	}
	if opts.BucketCount != 16 {
		t.Fatalf("expected BucketCount=16, got %d", opts.BucketCount)
	}
	if opts.CapPerBucket != 512 {
		t.Fatalf("expected CapPerBucket=512, got %d", opts.CapPerBucket)
	}
	if opts.Level2Cap != 256 {
		t.Fatalf("expected Level2Cap=256, got %d", opts.Level2Cap)
	}
	if opts.CleanupInterval != time.Minute {
		t.Fatalf("expected CleanupInterval=1m, got %v", opts.CleanupInterval)
	}
	if opts.OnEvicted != nil {
		t.Fatal("expected OnEvicted=nil")
	}
}

// ======================== Store 接口多态 ========================

func TestStoreInterface_Polymorphism(t *testing.T) {
	// 验证 LRU 和 LRU2 都正确实现了 Store 接口
	stores := []Store{
		NewStore(LRU, Options{MaxBytes: 1024}),
		NewStore(LRU2, Options{BucketCount: 2, CapPerBucket: 10, Level2Cap: 10}),
	}

	for i, s := range stores {
		defer s.Close()

		s.Set("test", testValue{data: "value"})
		v, ok := s.Get("test")
		if !ok || v.Len() != len("value") {
			t.Fatalf("store[%d]: basic Set/Get failed", i)
		}

		s.Delete("test")
		if s.Len() != 0 {
			t.Fatalf("store[%d]: Delete failed", i)
		}
	}
}

// ======================== Value 接口 ========================

func TestValueInterface(t *testing.T) {
	v := testValue{data: "hello world"}
	if v.Len() != len("hello world") {
		t.Fatalf("testValue.Len() should return %d, got %d", len("hello world"), v.Len())
	}

	// 空值
	v2 := testValue{data: ""}
	if v2.Len() != 0 {
		t.Fatalf("empty testValue.Len() should return 0, got %d", v2.Len())
	}

	// testValue 应满足 Value 接口（编译期保证）
	var _ Value = testValue{data: "x"}
}
