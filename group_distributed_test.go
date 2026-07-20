package reachcache

import (
	"context"
	"errors"
	"testing"
	"time"
)

// ======================== Get — 远端路由 ========================

func TestGroup_Get_RemoteHit(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{
		getFunc: func(_ context.Context, group, key string) ([]byte, error) {
			return []byte("remote-data"), nil
		},
	}
	var pickCount int
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			pickCount++
			return mp, true, false
		},
	}

	g := newTestGroup("test-remote-hit",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			t.Fatal("getter should not be called on remote hit")
			return nil, nil
		}),
		WithPeers(mpp),
	)

	v, err := g.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v.ByteSlice()) != "remote-data" {
		t.Fatalf("expected 'remote-data', got '%s'", string(v.ByteSlice()))
	}
	if pickCount != 1 {
		t.Fatalf("PickPeer should be called once, got %d", pickCount)
	}

	s := g.Stats()
	if s["peer_hits"].(int64) != 1 {
		t.Fatalf("peer_hits should be 1, got %d", s["peer_hits"].(int64))
	}
}

func TestGroup_Get_RemoteMiss_FallbackToGetter(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{
		getFunc: func(_ context.Context, group, key string) ([]byte, error) {
			return nil, errors.New("remote error")
		},
	}
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return mp, true, false
		},
	}

	var getterCalled bool
	g := newTestGroup("test-remote-miss",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			getterCalled = true
			return []byte("from-getter"), nil
		}),
		WithPeers(mpp),
	)

	v, err := g.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v.ByteSlice()) != "from-getter" {
		t.Fatalf("expected 'from-getter', got '%s'", string(v.ByteSlice()))
	}
	if !getterCalled {
		t.Fatal("getter should be called after peer miss")
	}

	s := g.Stats()
	if s["peer_misses"].(int64) != 1 {
		t.Fatalf("peer_misses should be 1, got %d", s["peer_misses"].(int64))
	}
	if s["loader_hits"].(int64) != 1 {
		t.Fatalf("loader_hits should be 1, got %d", s["loader_hits"].(int64))
	}
}

func TestGroup_Get_KeyBelongsToSelf(t *testing.T) {
	defer cleanupGroups()

	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return nil, true, true
		},
	}

	var getterCalled bool
	g := newTestGroup("test-self",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			getterCalled = true
			return []byte("self-data"), nil
		}),
		WithPeers(mpp),
	)

	v, err := g.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v.ByteSlice()) != "self-data" {
		t.Fatalf("expected 'self-data', got '%s'", string(v.ByteSlice()))
	}
	if !getterCalled {
		t.Fatal("getter should be called when key belongs to self")
	}

	s := g.Stats()
	if s["peer_hits"].(int64) != 0 {
		t.Fatalf("peer_hits should be 0 for self-routed key, got %d", s["peer_hits"].(int64))
	}
	if s["loader_hits"].(int64) != 1 {
		t.Fatalf("loader_hits should be 1, got %d", s["loader_hits"].(int64))
	}
}

func TestGroup_Get_PeerPickerNil(t *testing.T) {
	defer cleanupGroups()

	// peers == nil → single mode → always goes to Getter
	var getterCalled bool
	g := newTestGroup("test-nil-peer",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			getterCalled = true
			return []byte("local"), nil
		}),
	)

	v, err := g.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(v.ByteSlice()) != "local" {
		t.Fatalf("expected 'local', got '%s'", string(v.ByteSlice()))
	}
	if !getterCalled {
		t.Fatal("getter should be called when peers is nil")
	}
}

func TestGroup_Get_RemoteHit_BackfillCache(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{
		getFunc: func(_ context.Context, group, key string) ([]byte, error) {
			return []byte("remote-data"), nil
		},
	}
	var pickCount int
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			pickCount++
			return mp, true, false
		},
	}

	g := newTestGroup("test-backfill",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			t.Fatal("getter should not be called")
			return nil, nil
		}),
		WithPeers(mpp),
	)

	// 1st Get — miss → PickPeer → remote hit → backfill
	v1, err := g.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}
	if string(v1.ByteSlice()) != "remote-data" {
		t.Fatalf("expected 'remote-data', got '%s'", string(v1.ByteSlice()))
	}

	// 2nd Get — should be local hit, no PickPeer
	v2, err := g.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("second Get failed: %v", err)
	}
	if string(v2.ByteSlice()) != "remote-data" {
		t.Fatalf("expected 'remote-data', got '%s'", string(v2.ByteSlice()))
	}
	if pickCount != 1 {
		t.Fatalf("PickPeer should be called only once (backfill), got %d", pickCount)
	}

	s := g.Stats()
	if s["local_hits"].(int64) != 1 {
		t.Fatalf("local_hits should be 1 after backfill, got %d", s["local_hits"].(int64))
	}
}

// ======================== Set / Delete — 异步同步 ========================

func TestGroup_Set_SyncToPeer(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{setCalled: make(chan struct{}, 1)}
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return mp, true, false
		},
	}

	g := newTestGroup("test-sync-set",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("dummy"), nil
		}),
		WithPeers(mpp),
	)

	if err := g.Set(context.Background(), "mykey", []byte("myvalue")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	select {
	case <-mp.setCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for syncToPeers Set")
	}
}

func TestGroup_Delete_SyncToPeer(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{deleteCalled: make(chan struct{}, 1)}
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return mp, true, false
		},
	}

	g := newTestGroup("test-sync-del",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("dummy"), nil
		}),
		WithPeers(mpp),
	)

	g.Set(context.Background(), "mykey", []byte("v"))
	if err := g.Delete(context.Background(), "mykey"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	select {
	case <-mp.deleteCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for syncToPeers Delete")
	}
}

func TestGroup_Set_FromPeer_NoSync(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{setCalled: make(chan struct{}, 1)}
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return mp, true, false
		},
	}

	g := newTestGroup("test-from-peer-set",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("dummy"), nil
		}),
		WithPeers(mpp),
	)

	ctx := context.WithValue(context.Background(), "from_peer", true)
	if err := g.Set(ctx, "mykey", []byte("myvalue")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	select {
	case <-mp.setCalled:
		t.Fatal("syncToPeers should NOT be called for from_peer requests")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestGroup_Delete_FromPeer_NoSync(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{deleteCalled: make(chan struct{}, 1)}
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return mp, true, false
		},
	}

	g := newTestGroup("test-from-peer-del",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("dummy"), nil
		}),
		WithPeers(mpp),
	)

	ctx := context.WithValue(context.Background(), "from_peer", true)
	if err := g.Delete(ctx, "mykey"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	select {
	case <-mp.deleteCalled:
		t.Fatal("syncToPeers should NOT be called for from_peer requests")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestGroup_Set_SyncToPeer_IsSelf(t *testing.T) {
	defer cleanupGroups()

	mp := &mockPeer{setCalled: make(chan struct{}, 1)}
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return mp, true, true // self=true → no sync
		},
	}

	g := newTestGroup("test-sync-self",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("dummy"), nil
		}),
		WithPeers(mpp),
	)

	if err := g.Set(context.Background(), "mykey", []byte("v")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	select {
	case <-mp.setCalled:
		t.Fatal("syncToPeers should NOT be called when key belongs to self")
	case <-time.After(100 * time.Millisecond):
	}
}
