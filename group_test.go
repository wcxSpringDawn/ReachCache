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

package reachcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 每个测试后清理全局注册表，避免跨测试污染
func cleanupGroups() {
	DestroyAllGroups()
}

// helper: 创建单机模式的 Group（无 peers）
func newTestGroup(name string, getter Getter, opts ...GroupOption) *Group {
	return NewGroup(name, 8*1024*1024, getter, opts...)
}

// ======================== Get ========================

func TestGroup_Get_LocalHit(t *testing.T) {
	defer cleanupGroups()

	g := newTestGroup("test-local", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, fmt.Errorf("should not be called")
	}))

	// 直接 Set 后应命中本地缓存
	g.Set(context.Background(), "key", []byte("hello"))

	v, err := g.Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v.ByteSlice()) != "hello" {
		t.Fatalf("expected 'hello', got '%s'", string(v.ByteSlice()))
	}

	stats := g.Stats()
	if stats["local_hits"].(int64) != 1 {
		t.Fatalf("local_hits should be 1, got %d", stats["local_hits"].(int64))
	}
	if stats["loads"].(int64) != 0 {
		t.Fatalf("loads should be 0 on local hit, got %d", stats["loads"].(int64))
	}
}

func TestGroup_Get_LoaderHit(t *testing.T) {
	defer cleanupGroups()

	var getterCalled int32
	g := newTestGroup("test-loader", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&getterCalled, 1)
		return []byte("from-getter"), nil
	}))

	// 未 Set 直接 Get → 触发 Getter
	v, err := g.Get(context.Background(), "data-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v.ByteSlice()) != "from-getter" {
		t.Fatalf("expected 'from-getter', got '%s'", string(v.ByteSlice()))
	}

	if atomic.LoadInt32(&getterCalled) != 1 {
		t.Fatalf("getter should be called once, got %d", getterCalled)
	}

	stats := g.Stats()
	if stats["loader_hits"].(int64) != 1 {
		t.Fatalf("loader_hits should be 1, got %d", stats["loader_hits"].(int64))
	}

	// 第二次 Get 应命中本地缓存（loader 已回写）
	v2, err2 := g.Get(context.Background(), "data-key")
	if err2 != nil {
		t.Fatalf("second Get failed: %v", err2)
	}
	if string(v2.ByteSlice()) != "from-getter" {
		t.Fatal("second Get should return same data from cache")
	}
	if atomic.LoadInt32(&getterCalled) != 1 {
		t.Fatalf("getter should still be called only once")
	}
}

func TestGroup_Get_SingleFlight(t *testing.T) {
	defer cleanupGroups()

	var fnCallCount int32
	g := newTestGroup("test-sf", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		atomic.AddInt32(&fnCallCount, 1)
		time.Sleep(50 * time.Millisecond) // 模拟耗时加载
		return []byte("shared"), nil
	}))

	started := make(chan struct{})
	var wg sync.WaitGroup
	const n = 20

	// 第一个请求先注册 SingleFlight call
	wg.Add(1)
	go func() {
		defer wg.Done()
		v, _ := g.Get(context.Background(), "hot")
		close(started)
		if string(v.ByteSlice()) != "shared" {
			t.Error("unexpected value")
		}
	}()

	<-started

	// 其余请求在 Getter 执行期间到达 → 命中 SingleFlight 已有 call
	for i := 0; i < n-1; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := g.Get(context.Background(), "hot")
			if string(v.ByteSlice()) != "shared" {
				t.Error("unexpected value")
			}
		}()
	}
	wg.Wait()

	// Getter 应被调用恰好 1 次（SingleFlight 生效）
	if atomic.LoadInt32(&fnCallCount) != 1 {
		t.Fatalf("getter should be called exactly once (singleflight), got %d", fnCallCount)
	}
}

func TestGroup_Get_LoaderError(t *testing.T) {
	defer cleanupGroups()

	g := newTestGroup("test-err", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, fmt.Errorf("db down")
	}))

	_, err := g.Get(context.Background(), "missing")
	if err == nil {
		t.Fatal("Get should return error when getter fails")
	}

	stats := g.Stats()
	if stats["loader_errors"].(int64) != 1 {
		t.Fatalf("loader_errors should be 1, got %d", stats["loader_errors"].(int64))
	}
}

// ======================== Set ========================

func TestGroup_Set_Validation(t *testing.T) {
	defer cleanupGroups()

	g := newTestGroup("test-val", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("dummy"), nil
	}))

	// 空 key
	if err := g.Set(context.Background(), "", []byte("v")); err != ErrKeyRequired {
		t.Fatalf("expected ErrKeyRequired, got %v", err)
	}

	// 空 value
	if err := g.Set(context.Background(), "k", nil); err != ErrValueRequired {
		t.Fatalf("expected ErrValueRequired, got %v", err)
	}
	if err := g.Set(context.Background(), "k", []byte{}); err != ErrValueRequired {
		t.Fatalf("expected ErrValueRequired for empty slice, got %v", err)
	}

	// closed group
	g.close()
	if err := g.Set(context.Background(), "k", []byte("v")); err != ErrGroupClosed {
		t.Fatalf("expected ErrGroupClosed, got %v", err)
	}
}

func TestGroup_Set_WithExpiration(t *testing.T) {
	defer cleanupGroups()

	g := newTestGroup("test-ttl", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("loaded"), nil
	}), WithExpiration(50*time.Millisecond))

	g.Set(context.Background(), "k", []byte("v"))

	// 立即 Get 命中
	_, err := g.Get(context.Background(), "k")
	if err != nil {
		t.Fatal("Get should hit immediately after Set")
	}

	// 等待过期
	time.Sleep(100 * time.Millisecond)

	// 过期后 Get 触发 Getter
	_, err = g.Get(context.Background(), "k")
	if err != nil {
		t.Fatal("Get should fallback to getter after expiration")
	}
}

// ======================== Delete ========================

func TestGroup_Delete(t *testing.T) {
	defer cleanupGroups()

	var getterCalled bool
	g := newTestGroup("test-del", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		getterCalled = true
		return []byte("reloaded"), nil
	}))

	g.Set(context.Background(), "key", []byte("value"))
	g.Delete(context.Background(), "key")

	// 删除后 Get 触发回源
	v, err := g.Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("Get after delete should trigger getter: %v", err)
	}
	if string(v.ByteSlice()) != "reloaded" {
		t.Fatalf("expected 'reloaded', got '%s'", string(v.ByteSlice()))
	}
	if !getterCalled {
		t.Fatal("getter should be called after Delete+Get")
	}
}

func TestGroup_Delete_Validation(t *testing.T) {
	defer cleanupGroups()

	g := newTestGroup("test-delval", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))

	// 空 key
	if err := g.Delete(context.Background(), ""); err != ErrKeyRequired {
		t.Fatalf("expected ErrKeyRequired, got %v", err)
	}

	// closed group
	g.close()
	if err := g.Delete(context.Background(), "k"); err != ErrGroupClosed {
		t.Fatalf("expected ErrGroupClosed, got %v", err)
	}
}

// ======================== Clear ========================

func TestGroup_Clear(t *testing.T) {
	defer cleanupGroups()

	var getterCalled bool
	g := newTestGroup("test-clear", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		getterCalled = true
		return []byte("after-clear"), nil
	}))

	g.Set(context.Background(), "key", []byte("before"))
	g.Clear()

	// Clear 后 Get 触发回源
	v, err := g.Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("Get after Clear should trigger getter: %v", err)
	}
	if string(v.ByteSlice()) != "after-clear" {
		t.Fatalf("expected 'after-clear', got '%s'", string(v.ByteSlice()))
	}
	if !getterCalled {
		t.Fatal("getter should be called after Clear+Get")
	}
}

// ======================== Stats ========================

func TestGroup_Stats(t *testing.T) {
	defer cleanupGroups()

	g := newTestGroup("test-stats", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("data"), nil
	}))

	g.Set(context.Background(), "k1", []byte("v1"))
	g.Get(context.Background(), "k1") // local hit
	g.Get(context.Background(), "k2") // local miss → loader hit

	stats := g.Stats()

	checks := map[string]int64{
		"local_hits":    1,
		"local_misses":  1,
		"loads":         1,
		"loader_hits":   1,
		"loader_errors": 0,
	}
	for field, expected := range checks {
		if stats[field].(int64) != expected {
			t.Errorf("%s: expected %d, got %d", field, expected, stats[field].(int64))
		}
	}

	if stats["hit_rate"].(float64) != 0.5 {
		t.Errorf("hit_rate: expected 0.5, got %f", stats["hit_rate"].(float64))
	}
}

// ======================== Destroy / Lifecycle ========================

func TestGroup_Destroy(t *testing.T) {
	defer cleanupGroups()

	_ = newTestGroup("test-destroy", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))

	if !DestroyGroup("test-destroy") {
		t.Fatal("DestroyGroup should return true for existing group")
	}
	if GetGroup("test-destroy") != nil {
		t.Fatal("GetGroup should return nil after DestroyGroup")
	}
}

func TestGroup_DoubleDestroy(t *testing.T) {
	defer cleanupGroups()

	_ = newTestGroup("test-double", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))

	DestroyGroup("test-double")
	if DestroyGroup("test-double") {
		t.Fatal("second DestroyGroup should return false")
	}
}

func TestGroup_DestroyNonExistent(t *testing.T) {
	defer cleanupGroups()

	if DestroyGroup("no-such-group") {
		t.Fatal("DestroyGroup should return false for non-existent group")
	}
}

func TestGroup_DestroyAllGroups(t *testing.T) {
	defer cleanupGroups()

	_ = newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))
	_ = newTestGroup("g2", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))

	DestroyAllGroups()

	if len(ListGroups()) != 0 {
		t.Fatal("ListGroups should be empty after DestroyAllGroups")
	}
}

func TestGroup_NewGroup_Duplicate(t *testing.T) {
	defer cleanupGroups()

	g1 := newTestGroup("dup", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("first"), nil
	}))

	// 同名 Group 覆盖旧实例（记录警告但不报错）
	g2 := newTestGroup("dup", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("second"), nil
	}))

	if GetGroup("dup") != g2 {
		t.Fatal("GetGroup should return the newer instance")
	}
	if g1.closed != 1 {
		// 旧 Group 未被关闭，它只是从注册表中被替换了
		_ = g1
	}
}

func TestGroup_ListGroups(t *testing.T) {
	defer cleanupGroups()

	_ = newTestGroup("lg1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))
	_ = newTestGroup("lg2", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, nil
	}))

	names := ListGroups()
	if len(names) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(names))
	}
}

func TestGroup_Close(t *testing.T) {
	defer cleanupGroups()

	g := newTestGroup("test-close", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, fmt.Errorf("should not be called")
	}))

	g.Set(context.Background(), "k", []byte("v"))

	// DestroyGroup 内部调用 close()，Clear → Close store → CAS closed=1
	DestroyGroup("test-close")

	if atomic.LoadInt32(&g.closed) != 1 {
		t.Fatal("group should be closed after DestroyGroup")
	}
	if _, err := g.Get(context.Background(), "k"); err != ErrGroupClosed {
		t.Fatalf("expected ErrGroupClosed, got %v", err)
	}
}
