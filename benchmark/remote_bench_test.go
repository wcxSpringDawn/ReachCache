package bench_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	reachcache "github.com/vernmorn/reachcache"
	"github.com/vernmorn/reachcache/consistenthash"
	proto "github.com/vernmorn/reachcache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var cloudAddr = os.Getenv("CLOUD_ADDR")

type remotePicker struct {
	ring      *consistenthash.Map
	localAddr string
	remotes   map[string]*remotePeer
}

func newRemotePicker(localAddr string, remoteAddrs []string) *remotePicker {
	rp := &remotePicker{
		ring:      consistenthash.New(),
		localAddr: localAddr,
		remotes:   make(map[string]*remotePeer),
	}
	rp.ring.Add(localAddr)
	for _, addr := range remoteAddrs {
		rp.ring.Add(addr)
		rp.remotes[addr] = newRemotePeer(addr)
	}
	return rp
}

func (p *remotePicker) PickPeer(key string) (reachcache.Peer, bool, bool) {
	addr := p.ring.Get(key)
	if addr == "" {
		return nil, false, false
	}
	if addr == p.localAddr {
		return nil, true, true
	}
	if peer, ok := p.remotes[addr]; ok {
		return peer, true, false
	}
	return nil, false, false
}

func (p *remotePicker) Close() error {
	for _, peer := range p.remotes {
		_ = peer.Close()
	}
	return nil
}

type remotePeer struct {
	addr   string
	conn   *grpc.ClientConn
	client proto.ReachCacheClient
	mu     sync.Mutex
}

func newRemotePeer(addr string) *remotePeer {
	return &remotePeer{addr: addr}
}

func (p *remotePeer) ensureConn() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return
	}
	conn, err := grpc.NewClient(p.addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 3 * time.Second,
		}),
	)
	if err != nil {
		panic(fmt.Sprintf("failed to dial %s: %v", p.addr, err))
	}
	p.conn = conn
	p.client = proto.NewReachCacheClient(conn)
}

func (p *remotePeer) Get(group, key string) ([]byte, error) {
	p.ensureConn()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := p.client.Get(ctx, &proto.GetRequest{Group: group, Key: key})
	if err != nil {
		return nil, err
	}
	return resp.GetValue(), nil
}

func (p *remotePeer) Set(ctx context.Context, group, key string, value []byte) error {
	p.ensureConn()
	_, err := p.client.Set(ctx, &proto.SetRequest{Group: group, Key: key, Value: value})
	return err
}

func (p *remotePeer) Delete(ctx context.Context, group, key string) (bool, error) {
	p.ensureConn()
	resp, err := p.client.Delete(ctx, &proto.DeleteRequest{Group: group, Key: key})
	if err != nil {
		return false, err
	}
	return resp.Code == 0, nil
}

func (p *remotePeer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		return p.conn.Close()
	}
	return nil
}

func findKeyForRemote(ring *consistenthash.Map, remoteAddr string) string {
	for i := 0; i < 10000; i++ {
		key := "route-" + itoa(i)
		if ring.Get(key) == remoteAddr {
			return key
		}
	}
	return ""
}

func BenchmarkRemoteGet(b *testing.B) {
	if cloudAddr == "" {
		b.Skip("CLOUD_ADDR not set")
	}

	localAddr := "local-bench"
	picker := newRemotePicker(localAddr, []string{cloudAddr})

	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	}, reachcache.WithPeers(picker))
	defer cleanupBenchGroups()

	key := findKeyForRemote(picker.ring, cloudAddr)
	if key == "" {
		b.Fatal("could not find a key that routes to the cloud node")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, err := g.Get(context.Background(), key)
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
	})
}

func BenchmarkRemoteGet_PayloadSize(b *testing.B) {
	if cloudAddr == "" {
		b.Skip("CLOUD_ADDR not set")
	}

	sizes := []int{64, 256, 1024, 4096}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
				return makeValue(size), nil
			})
			defer cleanupBenchGroups()

			localAddr := "local-" + itoa(size)
			picker := newRemotePicker(localAddr, []string{cloudAddr})

			key := findKeyForRemote(picker.ring, cloudAddr)
			if key == "" {
				b.Fatal("could not find a key that routes to cloud")
			}

			_ = g.Set(context.Background(), key, makeValue(size))
			g2 := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
				return makeValue(size), nil
			}, reachcache.WithPeers(picker))
			defer cleanupBenchGroups()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					v, err := g2.Get(context.Background(), key)
					if err != nil {
						b.Fatal(err)
					}
					sink = v
				}
			})
		})
	}
}

func BenchmarkDistributedMix(b *testing.B) {
	if cloudAddr == "" {
		b.Skip("CLOUD_ADDR not set")
	}

	localAddr := "local-mix"
	picker := newRemotePicker(localAddr, []string{cloudAddr})

	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	}, reachcache.WithPeers(picker))
	defer cleanupBenchGroups()

	remoteKey := findKeyForRemote(picker.ring, cloudAddr)
	if remoteKey == "" {
		b.Fatal("could not find remote key")
	}

	localKeys := benchKeys("local-", 100)
	for _, k := range localKeys {
		_ = g.Set(context.Background(), k, makeValue(defaultPayloadSize))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := localKeys[time.Now().Nanosecond()%len(localKeys)]
			v, err := g.Get(context.Background(), key)
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
	})
}

func BenchmarkDistributed_RemoteWithBackfill(b *testing.B) {
	if cloudAddr == "" {
		b.Skip("CLOUD_ADDR not set")
	}

	localAddr := "local-backfill"
	picker := newRemotePicker(localAddr, []string{cloudAddr})

	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	}, reachcache.WithPeers(picker))
	defer cleanupBenchGroups()

	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = "bf-" + itoa(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := g.Get(context.Background(), keys[i])
		if err != nil {
			b.Fatal(err)
		}
		sink = v
	}
}

func BenchmarkDistributed_RemoteSetSync(b *testing.B) {
	if cloudAddr == "" {
		b.Skip("CLOUD_ADDR not set")
	}

	localAddr := "local-setsync"
	picker := newRemotePicker(localAddr, []string{cloudAddr})

	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	}, reachcache.WithPeers(picker))
	defer cleanupBenchGroups()

	val := makeValue(defaultPayloadSize)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "sync-" + itoa(i)
		_ = g.Set(context.Background(), key, val)
	}
}
