package reachcache

import (
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/vernmorn/reachcache/consistenthash"
)

// ======================== PickPeer ========================

func TestPickPeer_Remote(t *testing.T) {
	picker := newTestPicker("self:9999", map[string]*Client{
		"remote:8001": {addr: "remote:8001"},
	})

	peer, ok, self := picker.PickPeer("some-key")
	if !ok {
		t.Fatal("expected to find a peer")
	}
	if self {
		t.Fatal("should not be self for a remote key")
	}
	if peer == nil {
		t.Fatal("peer should not be nil")
	}
}

func TestPickPeer_Self(t *testing.T) {
	picker := newTestPicker("self:9999", map[string]*Client{
		"self:9999": {addr: "self:9999"},
	})

	// 选择一个一定会路由到 self:9999 的 key
	var ok bool
	var self bool
	for _, candidate := range []string{"key-for-self", "a", "b", "c", "mykey", "test", "self-key"} {
		_, ok, self = picker.PickPeer(candidate)
		if ok && self {
			break
		}
	}
	if !ok {
		t.Fatal("expected to find a peer for at least one key")
	}
	if !self {
		t.Fatal("key that routes to self should return self=true")
	}

	if !ok {
		t.Fatal("expected to find a peer")
	}
	if !self {
		t.Fatal("key that routes to self should return self=true")
	}
}

func TestPickPeer_EmptyRing(t *testing.T) {
	picker := newTestPicker("self:9999", nil)

	_, ok, _ := picker.PickPeer("any-key")
	if ok {
		t.Fatal("empty ring should return ok=false")
	}
}

func TestPickPeer_NoMatchingClient(t *testing.T) {
	picker := &ClientPicker{
		selfAddr: "self:9999",
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
	}
	// consHash 有节点，但 clients 中无对应 client
	picker.consHash.Add("ghost:8001")

	_, ok, _ := picker.PickPeer("any-key")
	if ok {
		t.Fatal("should return ok=false when no client matches the hash entry")
	}
}

// ======================== handleWatchEvents (DELETE) ========================

func TestHandleWatchEvents_Delete(t *testing.T) {
	picker := &ClientPicker{
		selfAddr: "self:9999",
		svcName:  "test-svc",
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
	}

	// 预先添加一个远端节点
	picker.consHash.Add("remote:8001")
	picker.clients["remote:8001"] = &Client{addr: "remote:8001"}

	// 模拟 DELETE 事件
	key := "/services/test-svc/remote:8001"
	events := []*clientv3.Event{
		{
			Type: clientv3.EventTypeDelete,
			Kv:   &mvccpb.KeyValue{Key: []byte(key)},
		},
	}

	picker.handleWatchEvents(events)

	if _, exists := picker.clients["remote:8001"]; exists {
		t.Fatal("client should be removed after DELETE event")
	}
}

func TestHandleWatchEvents_Delete_SkipSelf(t *testing.T) {
	picker := &ClientPicker{
		selfAddr: "self:9999",
		svcName:  "test-svc",
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
	}

	// Self 节点不会被 handleWatchEvents 管理
	key := "/services/test-svc/self:9999"
	events := []*clientv3.Event{
		{
			Type: clientv3.EventTypeDelete,
			Kv:   &mvccpb.KeyValue{Key: []byte(key)},
		},
	}

	picker.handleWatchEvents(events) // 不应 panic
}
