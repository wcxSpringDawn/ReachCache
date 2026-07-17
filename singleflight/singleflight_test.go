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

package singleflight

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ======================== 基本功能 ========================

func TestDo_SingleCall(t *testing.T) {
	var g Group

	v, err := g.Do("key", func() (interface{}, error) {
		return "result", nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "result" {
		t.Fatalf("expected 'result', got %v", v)
	}
}

func TestDo_ErrorPropagation(t *testing.T) {
	var g Group

	wantErr := errors.New("load failed")
	_, err := g.Do("key", func() (interface{}, error) {
		return nil, wantErr
	})

	if err != wantErr {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

// ======================== 并发请求合并 ========================

func TestDo_ConcurrentSameKey(t *testing.T) {
	var g Group
	var callCount int32
	const n = 100

	started := make(chan struct{}) // 首请求完成 Store 的信号
	var wg sync.WaitGroup

	// 第一个请求单独先跑，确保它完成 Load→Store 注册
	wg.Add(1)
	go func() {
		defer wg.Done()
		v, err := g.Do("hot-key", func() (interface{}, error) {
			atomic.AddInt32(&callCount, 1)
			close(started) // Store 已完成，通知后续请求可以开始
			time.Sleep(50 * time.Millisecond)
			return "shared", nil
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if v != "shared" {
			t.Errorf("expected 'shared', got %v", v)
		}
	}()

	<-started // 等待第一个请求的 call 注册完毕

	// 此时 Load 一定能命中已有 call，全部走等待路径
	for i := 0; i < n-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := g.Do("hot-key", func() (interface{}, error) {
				atomic.AddInt32(&callCount, 1)
				return "should-NOT-execute", nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if v != "shared" {
				t.Errorf("expected 'shared', got %v", v)
			}
		}()
	}
	wg.Wait()

	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("fn should be called exactly once, got %d", callCount)
	}
}

func TestDo_DifferentKeys(t *testing.T) {
	var g Group
	var callCount int32
	const n = 20

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", idx)
			_, err := g.Do(key, func() (interface{}, error) {
				atomic.AddInt32(&callCount, 1)
				return key, nil
			})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if atomic.LoadInt32(&callCount) != n {
		t.Fatalf("fn should be called %d times (once per key), got %d", n, callCount)
	}
}

// ======================== 结果共享 ========================

func TestDo_SharedResultCrossBatch(t *testing.T) {
	// 验证：Do 完成后 key 从 map 中删除，跨批次调用重新执行 fn
	var g Group

	v1, _ := g.Do("key", func() (interface{}, error) {
		return "first", nil
	})

	var fnCalled bool
	v2, _ := g.Do("key", func() (interface{}, error) {
		fnCalled = true
		return "second", nil
	})

	if !fnCalled {
		t.Fatal("fn should be called again: key was cleaned up after first Do")
	}
	if v1 == v2 {
		t.Fatal("second call should produce a new result (not cached across batches)")
	}
}

func TestDo_ConcurrentSharedResult(t *testing.T) {
	var g Group
	const n = 50

	started := make(chan struct{})
	var firstResult interface{}
	var mu sync.Mutex
	results := make([]interface{}, 0, n)
	var wg sync.WaitGroup

	// 第一个请求先注册 call
	wg.Add(1)
	go func() {
		defer wg.Done()
		v, _ := g.Do("key", func() (interface{}, error) {
			close(started)
			time.Sleep(30 * time.Millisecond)
			return "only-once", nil
		})
		mu.Lock()
		results = append(results, v)
		firstResult = v
		mu.Unlock()
	}()

	<-started

	for i := 0; i < n-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := g.Do("key", func() (interface{}, error) {
				return "should-NOT-execute", nil
			})
			mu.Lock()
			results = append(results, v)
			mu.Unlock()
		}()
	}
	wg.Wait()

	for _, r := range results {
		if r != firstResult {
			t.Fatal("all goroutines should share the same result")
		}
	}
}

// ======================== Panic 恢复 ========================

func TestDo_PanicRecovery(t *testing.T) {
	var g Group

	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("panic should be re-thrown")
			}
			close(done)
		}()
		g.Do("panic-key", func() (interface{}, error) {
			panic("boom")
		})
	}()
	<-done
}

func TestDo_PanicWakesWaiters(t *testing.T) {
	// 验证：fn panic 时，defer 保证 wg.Done() + Delete(key)，所有等待者不被永久阻塞
	var g Group
	const n = 10

	var wg sync.WaitGroup
	completed := int32(0)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { recover() }()
			g.Do("panic-key", func() (interface{}, error) {
				panic("unexpected")
			})
			// 如果执行到这里说明没有永久阻塞
			atomic.AddInt32(&completed, 1)
		}()
	}
	wg.Wait()
}

// ======================== Key 清理 ========================

func TestDo_KeyCleanup(t *testing.T) {
	var g Group

	g.Do("key", func() (interface{}, error) {
		return "done", nil
	})

	var count int
	v, _ := g.Do("key", func() (interface{}, error) {
		count++
		return "second", nil
	})

	if count != 1 {
		t.Fatalf("fn should be called once on second invocation, got %d", count)
	}
	if v != "second" {
		t.Fatalf("expected 'second', got %v", v)
	}
}

// ======================== 边界条件 ========================

func TestDo_EmptyKey(t *testing.T) {
	var g Group

	v, err := g.Do("", func() (interface{}, error) {
		return "empty-key-ok", nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != "empty-key-ok" {
		t.Fatalf("unexpected value: %v", v)
	}
}

func TestDo_NilResult(t *testing.T) {
	var g Group

	v, err := g.Do("key", func() (interface{}, error) {
		return nil, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

// ======================== 并发混合场景 ========================

func TestDo_MixedKeys(t *testing.T) {
	var g Group
	const nKeys = 5
	const nPerKey = 20

	var callCount [nKeys]int32
	var started [nKeys]chan struct{}
	for k := 0; k < nKeys; k++ {
		started[k] = make(chan struct{})
	}

	var wg sync.WaitGroup

	// 每个 key 的第一个请求先注册 call
	for k := 0; k < nKeys; k++ {
		wg.Add(1)
		go func(keyIdx int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", keyIdx)
			g.Do(key, func() (interface{}, error) {
				atomic.AddInt32(&callCount[keyIdx], 1)
				close(started[keyIdx])
				time.Sleep(20 * time.Millisecond)
				return key, nil
			})
		}(k)
	}

	// 等待所有 key 的 call 注册完毕
	for k := 0; k < nKeys; k++ {
		<-started[k]
	}

	// 剩余请求此时 Load 一定命中已有 call
	for k := 0; k < nKeys; k++ {
		for i := 0; i < nPerKey-1; i++ {
			wg.Add(1)
			go func(keyIdx int) {
				defer wg.Done()
				key := fmt.Sprintf("k%d", keyIdx)
				g.Do(key, func() (interface{}, error) {
					atomic.AddInt32(&callCount[keyIdx], 1)
					return "should-NOT-execute", nil
				})
			}(k)
		}
	}
	wg.Wait()

	for k := 0; k < nKeys; k++ {
		if atomic.LoadInt32(&callCount[k]) != 1 {
			t.Errorf("key k%d: fn should be called exactly once, got %d", k, callCount[k])
		}
	}
}
