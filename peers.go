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
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/vernmorn/reachcache/consistenthash"
	"github.com/vernmorn/reachcache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultSvcName = "reach-cache"

// PeerPicker 是节点选择器的抽象接口，解耦路由策略与业务逻辑。
// 给定一个 key，PickPeer 返回应由哪个节点处理、是否找到该节点、是否为本地节点。
type PeerPicker interface {
	// PickPeer 根据 key 选择合适的缓存节点。
	// peer: 目标节点的操作接口；ok: 是否存在有效节点；self: 是否为当前节点自身。
	PickPeer(key string) (peer Peer, ok bool, self bool)

	// Close 关闭节点选择器，释放网络连接等资源。
	Close() error
}

// Peer 封装对单个缓存节点的操作能力，使上层可统一操作本地或远程节点。
// Client 是 Peer 接口的默认实现（通过 gRPC 通信）。
type Peer interface {
	Get(group string, key string) ([]byte, error)
	Set(ctx context.Context, group string, key string, value []byte) error
	Delete(ctx context.Context, group string, key string) (bool, error)
	Close() error
}

// ClientPicker 是 PeerPicker 的默认实现，整合了一致性哈希路由、gRPC 客户端管理和
// etcd 服务发现三大功能。通过 etcd Watch 实时感知集群拓扑变化，动态更新哈希环和连接池。
type ClientPicker struct {
	selfAddr      string              // 当前节点的地址（IP:Port），用于识别自身
	svcName       string              // 服务名称，用于在etcd中注册和发现服务实例
	etcdEndpoints []string            // etcd端点列表
	clientOpts    []ClientOption      // 传递给底层Client的连接选项（如TLS）
	mu            sync.RWMutex        // 读写锁，保证并发安全
	consHash      *consistenthash.Map // 一致性哈希实现，用于根据key选择节点
	clients       map[string]*Client  // 节点名到Client实例的映射，存储已发现的节点和对应的客户端连接
	etcdCli       *clientv3.Client    // etcd客户端，用于服务发现和监听节点变化
	ctx           context.Context     // 上下文，用于控制服务发现的生命周期
	cancel        context.CancelFunc  // 取消函数，用于停止服务发现和清理资源
}

// PickerOption 是 ClientPicker 的函数选项类型。
type PickerOption func(*ClientPicker)

// WithServiceName 设置 ClientPicker 使用的服务名称（对应 etcd 中的注册前缀）。
func WithServiceName(name string) PickerOption {
	return func(p *ClientPicker) {
		p.svcName = name
	}
}

// WithEtcdEndpointsForClientPicker 设置 ClientPicker 使用的 etcd 端点列表。
func WithEtcdEndpointsForClientPicker(endpoints []string) PickerOption {
	return func(p *ClientPicker) {
		if len(endpoints) > 0 {
			p.etcdEndpoints = append([]string(nil), endpoints...)
		}
	}
}

// WithClientOptions 为 ClientPicker 设置底层 Client 连接选项（如 WithClientTLS、WithClientAuthToken）。
func WithClientOptions(opts ...ClientOption) PickerOption {
	return func(p *ClientPicker) {
		if len(opts) > 0 {
			p.clientOpts = append([]ClientOption(nil), opts...)
		}
	}
}

// PrintPeers 打印当前已发现的节点（仅用于调试）
func (p *ClientPicker) PrintPeers() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	log.Printf("当前已发现的节点:")
	for addr := range p.clients {
		log.Printf("- %s", addr)
	}
}

// NewClientPicker 创建 ClientPicker 实例。
// 启动时执行"全量拉取 + Watch 监听"的双阶段服务发现：
//  1. fetchAllServices：一次性获取 etcd 中所有已注册节点
//  2. watchServiceChanges：后台 goroutine 持续监听节点上下线
func NewClientPicker(addr string, opts ...PickerOption) (*ClientPicker, error) {
	ctx, cancel := context.WithCancel(context.Background())
	picker := &ClientPicker{
		selfAddr:      addr,
		svcName:       defaultSvcName,
		etcdEndpoints: append([]string(nil), registry.DefaultConfig.Endpoints...),
		clients:       make(map[string]*Client),
		consHash:      consistenthash.New(),
		ctx:           ctx,
		cancel:        cancel,
	}

	for _, opt := range opts {
		opt(picker)
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   picker.etcdEndpoints,
		DialTimeout: registry.DefaultConfig.DialTimeout,
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}
	picker.etcdCli = cli

	// 启动服务发现
	if err := picker.startServiceDiscovery(); err != nil {
		cancel()
		cli.Close()
		return nil, err
	}

	return picker, nil
}

// startServiceDiscovery 启动服务发现
func (p *ClientPicker) startServiceDiscovery() error {
	// 先进行全量更新
	if err := p.fetchAllServices(); err != nil {
		return err
	}

	// 启动增量更新
	go p.watchServiceChanges()
	return nil
}

// watchServiceChanges 监听服务实例变化
func (p *ClientPicker) watchServiceChanges() {
	watcher := clientv3.NewWatcher(p.etcdCli)
	watchChan := watcher.Watch(p.ctx, "/services/"+p.svcName, clientv3.WithPrefix())

	for {
		select {
		case <-p.ctx.Done():
			watcher.Close()
			return
		case resp, ok := <-watchChan:
			if !ok {
				logrus.Warn("service discovery watch channel closed")
				watcher.Close()
				return
			}
			if err := resp.Err(); err != nil {
				logrus.Warnf("service discovery watch error: %v", err)
				continue
			}
			p.handleWatchEvents(resp.Events)
		}
	}
}

// handleWatchEvents 处理监听到的事件
func (p *ClientPicker) handleWatchEvents(events []*clientv3.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, event := range events {
		switch event.Type {
		case clientv3.EventTypePut:
			addr := string(event.Kv.Value)
			if addr == "" || addr == p.selfAddr {
				continue
			}
			p.set(addr)
			logrus.Infof("New service discovered at %s", addr)
		case clientv3.EventTypeDelete:
			addr := parseAddrFromKey(string(event.Kv.Key), p.svcName)
			if addr == "" || addr == p.selfAddr {
				continue
			}
			if client, exists := p.clients[addr]; exists {
				client.Close()
				p.remove(addr)
				logrus.Infof("Service removed at %s", addr)
			}
		}
	}
}

// fetchAllServices 一次性拉取 etcd 中 /services/{svcName}/ 前缀下的所有已注册节点。
func (p *ClientPicker) fetchAllServices() error {
	ctx, cancel := context.WithTimeout(p.ctx, 3*time.Second)
	defer cancel()

	resp, err := p.etcdCli.Get(ctx, "/services/"+p.svcName, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to get all services: %v", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, kv := range resp.Kvs {
		addr := string(kv.Value)
		if addr != "" && addr != p.selfAddr {
			p.set(addr)
			logrus.Infof("Discovered service at %s", addr)
		}
	}
	return nil
}

// set 添加服务实例。若该地址已有客户端，先关闭旧连接再创建新的。
// 滚动重启或 etcd 租约残留时，确保始终连接到最新实例。
func (p *ClientPicker) set(addr string) {
	if old, exists := p.clients[addr]; exists {
		old.Close()
		p.consHash.Remove(addr)
		delete(p.clients, addr)
		logrus.Infof("Closing stale client for %s", addr)
	}

	client, err := NewClient(addr, p.svcName, p.etcdCli, p.clientOpts...)
	if err != nil {
		logrus.Errorf("Failed to create client for %s: %v", addr, err)
		return
	}
	p.consHash.Add(addr)
	p.clients[addr] = client
	logrus.Infof("Successfully created client for %s", addr)
}

// remove 移除服务实例
func (p *ClientPicker) remove(addr string) {
	p.consHash.Remove(addr)
	delete(p.clients, addr)
}

// PickPeer 实现 PeerPicker 接口。通过一致性哈希选择 key 的归属节点，
// 查找对应的 Client 实例并返回。返回的三个值分别表示：目标节点、是否找到、是否为本地节点。
func (p *ClientPicker) PickPeer(key string) (Peer, bool, bool) {
	// 获取读锁，保证在选择节点时不会有并发修改
	p.mu.RLock()
	defer p.mu.RUnlock()

	// 使用一致性哈希选择节点名（节点地址）
	if addr := p.consHash.Get(key); addr != "" {
		// 查找对应地址的Client实例
		if client, ok := p.clients[addr]; ok {
			// 返回Client实例、是否找到节点、是否是本地节点
			return client, true, addr == p.selfAddr
		}
	}
	return nil, false, false
}

// Close 关闭 ClientPicker：取消服务发现 goroutine、关闭所有 Client 连接和 etcd 连接。
func (p *ClientPicker) Close() error {
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()

	p.consHash.Close()

	var errs []error
	for addr, client := range p.clients {
		if err := client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close client %s: %v", addr, err))
		}
	}

	if err := p.etcdCli.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close etcd client: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

// parseAddrFromKey 从etcd key中解析地址
func parseAddrFromKey(key, svcName string) string {
	prefix := fmt.Sprintf("/services/%s/", svcName)
	if strings.HasPrefix(key, prefix) {
		return strings.TrimPrefix(key, prefix)
	}
	return ""
}
