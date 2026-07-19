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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vernmorn/reachcache/singleflight"
)

// 全局 Group 注册表，通过名称索引。
// groupsMu 为 RWMutex：GetGroup 用读锁，NewGroup/DestroyGroup 用写锁。
var (
	groupsMu sync.RWMutex
	groups   = make(map[string]*Group)
)

// ErrKeyRequired 表示操作需要非空的 key。
var ErrKeyRequired = errors.New("key is required")

// ErrValueRequired 表示 Set 操作需要非空的 value。
var ErrValueRequired = errors.New("value is required")

// ErrGroupClosed 表示操作的目标 Group 已关闭。
var ErrGroupClosed = errors.New("cache group is closed")

// Group 是 ReachCache 的核心命名空间抽象。每个 Group 代表一个独立的缓存区域，
// 拥有隔离的存储实例、数据回源回调、过期时间配置和统计信息。
//
// 多 Group 解决了不同业务线缓存混放带来的数据混淆、相互干扰和运维困难问题。
//
// 核心流程（Get 为例）：
//
//	Group.Get → 查 mainCache → 命中则返回（localHits++）
//	         → 未命中 → load() → singleflight.Do → loadData()
//	             → 有 peers → PickPeer → 远端拉取（peerHits++）
//	             → 无 peers 或 key 属本节点 → Getter 回调（loaderHits++）
//	         → 回写 mainCache → 返回
type Group struct {
	name       string              // 唯一标识，缓存组名称，全局注册表的键
	getter     Getter              // 缓存未命中时的数据回源接口
	mainCache  *Cache              // 本地缓存实例
	peers      PeerPicker          // 分布式节点选择器（nil=单机模式）
	loader     *singleflight.Group // 请求合并器，防止缓存击穿
	expiration time.Duration       // 缓存项默认 TTL，0=永不过期
	closed     int32               // 原子变量：0=运行中，1=已关闭
	stats      groupStats          // 多维度统计信息
}

// groupStats 使用 int64 原子变量记录多维度运行时指标，全部通过 atomic 操作更新。
type groupStats struct {
	loads        int64 // 触发 load 流程的总次数
	localHits    int64 // 本地缓存命中次数
	localMisses  int64 // 本地缓存未命中次数
	peerHits     int64 // 从远端节点获取成功次数
	peerMisses   int64 // 从远端节点获取失败次数
	loaderHits   int64 // 从 Getter 加载器获取成功次数
	loaderErrors int64 // 从 Getter 加载器获取失败次数
	loadDuration int64 // 加载总耗时（纳秒），配合 loads 计算平均耗时
}

// Getter 定义数据回源加载接口。当缓存未命中且无法从远端节点获取时，
// 系统调用 Get(ctx, key) 从后端数据源（DB、RPC、文件、计算引擎等）加载数据。
// 每个 Group 必须配置一个 Getter。
type Getter interface {
	Get(ctx context.Context, key string) ([]byte, error)
}

// GetterFunc 是函数类型适配器，使得普通函数可通过类型转换满足 Getter 接口。
// 借鉴 http.HandlerFunc 的适配模式：func(ctx, key) ([]byte, error) → Getter。
type GetterFunc func(ctx context.Context, key string) ([]byte, error)

// Get 实现 Getter 接口。
func (f GetterFunc) Get(ctx context.Context, key string) ([]byte, error) {
	return f(ctx, key)
}

// GroupOption 是 Group 的配置选项函数类型，实现函数选项模式（Functional Options Pattern）。
// 优点：新增配置项无需修改 NewGroup 签名；选项函数名即文档；未传入的选项自动使用零值。
type GroupOption func(*Group)

// WithExpiration 设置 Group 中缓存项的默认 TTL。0 表示永不过期。
func WithExpiration(d time.Duration) GroupOption {
	return func(g *Group) {
		g.expiration = d
	}
}

// WithPeers 注入 PeerPicker 使 Group 具备分布式路由能力。
// 未设置时 peers 为 nil，Group 运行在单机模式下。
func WithPeers(peers PeerPicker) GroupOption {
	return func(g *Group) {
		g.peers = peers
		logrus.Infof("[ReachCache] registered peers for group [%s]", g.name)
	}
}

// WithCacheOptions 自定义底层缓存引擎的类型和参数。
// 可实现不同 Group 使用不同的淘汰策略（如用户 Group 用 LRU、商品 Group 用 LRU-2）。
func WithCacheOptions(opts CacheOptions) GroupOption {
	return func(g *Group) {
		g.mainCache = NewCache(opts)
	}
}

// NewGroup 创建一个 Group 实例并注册到全局表。
//
// 必填参数：
//   - name: 唯一标识，同名 Group 会被覆盖并记录警告
//   - cacheBytes: 本地缓存的内存上限（字节）
//   - getter: 数据回源回调，不能为 nil（否则 panic）
//
// 可选 opts: WithExpiration / WithPeers / WithCacheOptions
func NewGroup(name string, cacheBytes int64, getter Getter, opts ...GroupOption) *Group {
	if getter == nil {
		panic("nil Getter")
	}

	// 创建默认缓存选项
	cacheOpts := DefaultCacheOptions()
	cacheOpts.MaxBytes = cacheBytes

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: NewCache(cacheOpts),
		loader:    &singleflight.Group{},
	}

	// 应用选项函数
	for _, opt := range opts {
		opt(g)
	}

	// 注册到全局组映射
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if old, exists := groups[name]; exists {
		logrus.Warnf("Group with name %s already exists, will be replaced", name)
		old.close()
	}

	groups[name] = g
	logrus.Infof("Created cache group [%s] with cacheBytes=%d, expiration=%v", name, cacheBytes, g.expiration)

	return g
}

// GetGroup 通过名称从全局注册表中查找 Group。使用读锁，并发安全。
func GetGroup(name string) *Group {
	groupsMu.RLock()
	defer groupsMu.RUnlock()
	return groups[name]
}

// Get 从缓存获取数据，执行三级回源策略：
//  1. 查 mainCache → 命中返回（localHits++）
//  2. load() → singleflight.Do → loadData()
//     a. 有 peers 且 key 属远端 → gRPC 拉取（peerHits++）
//     b. 无 peers 或 key 属本节点 → Getter 回调（loaderHits++）
//  3. 回写 mainCache → 返回
func (g *Group) Get(ctx context.Context, key string) (ByteView, error) {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ByteView{}, ErrGroupClosed
	}

	if key == "" {
		return ByteView{}, ErrKeyRequired
	}

	// 从本地缓存获取
	view, ok := g.mainCache.Get(ctx, key)
	if ok {
		atomic.AddInt64(&g.stats.localHits, 1)
		return view, nil
	}

	atomic.AddInt64(&g.stats.localMisses, 1)

	// 尝试从其他节点获取或从数据源加载，具体逻辑在load方法中实现
	return g.load(ctx, key)
}

// Set 写入本地缓存并异步同步到远端归属节点（若启用分布式模式）。
// 校验：closed → ErrGroupClosed；空 key → ErrKeyRequired；空 value → ErrValueRequired。
// from_peer 上下文标记防止无限循环同步。
func (g *Group) Set(ctx context.Context, key string, value []byte) error {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}
	if len(value) == 0 {
		return ErrValueRequired
	}

	// 检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	// 创建缓存视图
	view := ByteView{b: cloneBytes(value)}

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	// 如果当前这个Set请求是从其它节点通过gRPC同步过来的，就不需要再同步到其它节点了，否则会造成无限循环的Set请求。
	// 如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到key的所属节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "set", key, value)
	}

	return nil
}

// Delete 从本地缓存删除并异步传播到远端归属节点（若启用分布式模式）。
func (g *Group) Delete(ctx context.Context, key string) error {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return ErrGroupClosed
	}

	if key == "" {
		return ErrKeyRequired
	}

	// 从本地缓存删除
	g.mainCache.Delete(key)

	// 检查是否是从其他节点同步过来的请求
	isPeerRequest := ctx.Value("from_peer") != nil

	// 如果当前这个Delete请求是从其它节点通过gRPC同步过来的，就不需要再同步到其它节点了，否则会造成无限循环的Delete请求。
	// 如果不是从其他节点同步过来的请求，且启用了分布式模式，同步到key的所属节点
	if !isPeerRequest && g.peers != nil {
		go g.syncToPeers(ctx, "delete", key, nil)
	}

	return nil
}

// Clear 清空当前 Group 的所有缓存数据。清空后 Group 仍可继续接受读写请求。
func (g *Group) Clear() {
	// 检查组是否已关闭
	if atomic.LoadInt32(&g.closed) == 1 {
		return
	}

	g.mainCache.Clear()
	logrus.Infof("[ReachCache] cleared cache for group [%s]", g.name)
}

// close 关闭 Group（不可逆）。CAS 保证幂等，内部自动 Clear 后 Close 底层存储。
// 未导出——仅由 DestroyGroup/DestroyAllGroups 调用，避免外部直接使用。
// 不在内部从全局注册表删除自身，防止 DestroyGroup 调用时的死锁。
func (g *Group) close() error {
	// 如果已经关闭，直接返回
	// CompareAndSwapInt32的操作是，如果当前值是0，则将其设置为1，并返回true；如果当前值不是0，则不修改它，并返回false。
	if !atomic.CompareAndSwapInt32(&g.closed, 0, 1) {
		return nil
	}

	// 先清空缓存数据，触发OnEvicted回调后再释放底层资源
	g.mainCache.Clear()

	// 关闭本地缓存，停止后台清理协程
	if g.mainCache != nil {
		g.mainCache.Close()
	}

	// 不应在这里将自己从全局组映射中删除，因为DestroyGroup函数可能会获得全局组映射的锁然后调用close方法来销毁组，此时发生死锁。相反，应该在DestroyGroup函数中先调用close方法来关闭组，然后再从全局组映射中删除。
	/*
		// 从全局组映射中移除
		groupsMu.Lock()
		delete(groups, g.name)
		groupsMu.Unlock()
	*/

	logrus.Infof("[ReachCache] closed cache group [%s]", g.name)
	return nil
}

// load 通过 singleflight.Do 包裹 loadData，保证同一 key 的并发请求只加载一次。
// 加载完成后回写到 mainCache（若设置了 expiration 则带 TTL）。
func (g *Group) load(ctx context.Context, key string) (value ByteView, err error) {
	// 使用 singleflight 确保并发请求只加载一次
	startTime := time.Now()
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		return g.loadData(ctx, key)
	})

	// 记录加载时间
	loadDuration := time.Since(startTime).Nanoseconds()
	atomic.AddInt64(&g.stats.loadDuration, loadDuration)
	atomic.AddInt64(&g.stats.loads, 1)

	if err != nil {
		atomic.AddInt64(&g.stats.loaderErrors, 1)
		return ByteView{}, err
	}

	view := viewi.(ByteView)

	// 设置到本地缓存
	if g.expiration > 0 {
		g.mainCache.AddWithExpiration(key, view, time.Now().Add(g.expiration))
	} else {
		g.mainCache.Add(key, view)
	}

	return view, nil
}

// loadData 执行实际的数据加载：优先从远端节点拉取，否则回退到 Getter 回调。
func (g *Group) loadData(ctx context.Context, key string) (value ByteView, err error) {
	// 尝试从节点获取
	if g.peers != nil {
		peer, ok, isSelf := g.peers.PickPeer(key)
		// 如果找到了key所在节点，并且不是自己，就尝试从该节点获取数据
		if ok && !isSelf {
			value, err := g.getFromPeer(ctx, peer, key)
			if err == nil {
				atomic.AddInt64(&g.stats.peerHits, 1)
				return value, nil
			}

			atomic.AddInt64(&g.stats.peerMisses, 1)
			logrus.Warnf("[ReachCache] failed to get from peer: %v", err)
		}
	}

	// 从数据源加载
	bytes, err := g.getter.Get(ctx, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get data: %w", err)
	}

	atomic.AddInt64(&g.stats.loaderHits, 1)
	return ByteView{b: cloneBytes(bytes)}, nil
}

// getFromPeer 通过 Peer 接口向远端节点发送 Get 请求。
func (g *Group) getFromPeer(ctx context.Context, peer Peer, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key)
	if err != nil {
		return ByteView{}, fmt.Errorf("failed to get from peer: %w", err)
	}
	return ByteView{b: bytes}, nil
}

// syncToPeers 通过一致性哈希确定 key 的归属节点，异步调用远端 Set/Delete。
// 两道防线防止无限循环：isSelf 检查（稳态）+ from_peer 标记（瞬态窗口兜底）。
func (g *Group) syncToPeers(ctx context.Context, op string, key string, value []byte) {
	// 如果没有配置节点选择器，直接返回
	if g.peers == nil {
		return
	}

	// 选择key所属的节点，如果选择的节点是自己，则不需要同步
	peer, ok, isSelf := g.peers.PickPeer(key)
	if !ok || isSelf {
		return
	}

	// 本地进程中gRPC的context value其实不会被传输到远端，这里只是形式上的标记，实际的标记是由Server.Set/Delete对所有入站请求统一打上from_peer标记。
	// 由于只有缓存节点自身能通过gRPC一元拦截器的token认证建立gRPC连接，因此Server.Set/Delete对所有入站请求统一打上from_peer标记是安全的。
	syncCtx := context.WithValue(context.Background(), "from_peer", true)

	var err error
	switch op {
	case "set":
		setCtx, cancel := context.WithTimeout(syncCtx, 3*time.Second)
		defer cancel()
		err = peer.Set(setCtx, g.name, key, value)
	case "delete":
		deleteCtx, cancel := context.WithTimeout(syncCtx, 3*time.Second)
		defer cancel()
		_, err = peer.Delete(deleteCtx, g.name, key)
	}

	if err != nil {
		logrus.Errorf("[ReachCache] failed to sync %s to peer: %v", op, err)
	}
}

// Stats 返回 Group 的多维度统计信息，包含本地/远端加载、命中率和缓存状态。
func (g *Group) Stats() map[string]interface{} {
	stats := map[string]interface{}{
		"name":          g.name,
		"closed":        atomic.LoadInt32(&g.closed) == 1,
		"expiration":    g.expiration,
		"loads":         atomic.LoadInt64(&g.stats.loads),
		"local_hits":    atomic.LoadInt64(&g.stats.localHits),
		"local_misses":  atomic.LoadInt64(&g.stats.localMisses),
		"peer_hits":     atomic.LoadInt64(&g.stats.peerHits),
		"peer_misses":   atomic.LoadInt64(&g.stats.peerMisses),
		"loader_hits":   atomic.LoadInt64(&g.stats.loaderHits),
		"loader_errors": atomic.LoadInt64(&g.stats.loaderErrors),
	}

	// 计算各种命中率
	totalGets := stats["local_hits"].(int64) + stats["local_misses"].(int64)
	if totalGets > 0 {
		stats["hit_rate"] = float64(stats["local_hits"].(int64)) / float64(totalGets)
	}

	totalLoads := stats["loads"].(int64)
	if totalLoads > 0 {
		stats["avg_load_time_ms"] = float64(atomic.LoadInt64(&g.stats.loadDuration)) / float64(totalLoads) / float64(time.Millisecond)
	}

	// 添加缓存大小
	if g.mainCache != nil {
		cacheStats := g.mainCache.Stats()
		for k, v := range cacheStats {
			stats["cache_"+k] = v
		}
	}

	return stats
}

// ListGroups 返回全局注册表中所有 Group 的名称列表。
func ListGroups() []string {
	groupsMu.RLock()
	defer groupsMu.RUnlock()

	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}

	return names
}

// DestroyGroup 关闭 Group 并从全局注册表移除。先 close() 再 delete(groups, name)。
// close 不自行删除注册表条目，避免锁重入死锁。
func DestroyGroup(name string) bool {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	if g, exists := groups[name]; exists {
		g.close()            // close内部会自动清空缓存
		delete(groups, name) // 从全局注册表移除
		logrus.Infof("[ReachCache] destroyed cache group [%s]", name)
		return true
	}

	return false
}

// DestroyAllGroups 销毁全局注册表中的所有 Group，常用于进程退出前的资源清理。
func DestroyAllGroups() {
	groupsMu.Lock()
	defer groupsMu.Unlock()

	for name, g := range groups {
		g.close()            // close内部会自动清空缓存
		delete(groups, name) // 从全局注册表移除
		logrus.Infof("[ReachCache] destroyed cache group [%s]", name)
	}
}
