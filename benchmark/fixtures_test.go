package bench_test

import (
	"context"
	"crypto/rand"
	"sync/atomic"

	reachcache "github.com/vernmorn/reachcache"
)

const defaultPayloadSize = 256

var sink interface{}

func makeValue(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}

type benchValue struct {
	data []byte
}

func (v benchValue) Len() int { return len(v.data) }

func benchKeys(prefix string, count int) []string {
	keys := make([]string, count)
	for i := range keys {
		keys[i] = prefix + itoa(i)
	}
	return keys
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

type benchPeer struct {
	getFunc    func(ctx context.Context, group, key string) ([]byte, error)
	setFunc    func(ctx context.Context, group, key string, value []byte) error
	deleteFunc func(ctx context.Context, group, key string) (bool, error)
}

func (p *benchPeer) Get(group, key string) ([]byte, error) {
	return p.getFunc(context.Background(), group, key)
}

func (p *benchPeer) Set(ctx context.Context, group, key string, value []byte) error {
	return p.setFunc(ctx, group, key, value)
}

func (p *benchPeer) Delete(ctx context.Context, group, key string) (bool, error) {
	return p.deleteFunc(ctx, group, key)
}

func (p *benchPeer) Close() error { return nil }

type benchPeerPicker struct {
	pickFunc func(key string) (peer reachcache.Peer, ok bool, self bool)
}

func (p *benchPeerPicker) PickPeer(key string) (reachcache.Peer, bool, bool) {
	return p.pickFunc(key)
}

func (p *benchPeerPicker) Close() error { return nil }

var groupID atomic.Int32

func nextGroupName() string {
	id := groupID.Add(1)
	return "bench-group-" + itoa(int(id))
}

func setupBenchGroup(getter func(ctx context.Context, key string) ([]byte, error), opts ...reachcache.GroupOption) *reachcache.Group {
	g := reachcache.NewGroup(nextGroupName(), 64<<20, reachcache.GetterFunc(getter), opts...)
	return g
}

func cleanupBenchGroups() {
	for _, name := range reachcache.ListGroups() {
		reachcache.DestroyGroup(name)
	}
}

