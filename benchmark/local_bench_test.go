package bench_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	reachcache "github.com/vernmorn/reachcache"
	"github.com/vernmorn/reachcache/singleflight"
	"github.com/vernmorn/reachcache/store"
)

func BenchmarkGroupGet_LocalHit(b *testing.B) {
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	})
	defer cleanupBenchGroups()

	key := "hit-key"
	val := makeValue(defaultPayloadSize)
	_ = g.Set(context.Background(), key, val)
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

func BenchmarkGroupGet_LocalHit_PayloadSize(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
				return makeValue(size), nil
			})
			defer cleanupBenchGroups()

			key := "hit-key"
			_ = g.Set(context.Background(), key, makeValue(size))
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					v, _ := g.Get(context.Background(), key)
					sink = v
				}
			})
		})
	}
}

func BenchmarkGroupGet_LoaderHit(b *testing.B) {
	var mu sync.Mutex
	getterCount := 0
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		mu.Lock()
		getterCount++
		mu.Unlock()
		return makeValue(defaultPayloadSize), nil
	})
	defer cleanupBenchGroups()

	keys := benchKeys("loader-", b.N)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			v, err := g.Get(context.Background(), keys[i%len(keys)])
			if err != nil {
				b.Fatal(err)
			}
			sink = v
			i++
		}
	})
}

func BenchmarkGroupSet(b *testing.B) {
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	})
	defer cleanupBenchGroups()

	val := makeValue(defaultPayloadSize)
	keys := benchKeys("set-", b.N)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = g.Set(context.Background(), keys[i%len(keys)], val)
			i++
		}
	})
}

func BenchmarkGroupGet_Miss(b *testing.B) {
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	})
	defer cleanupBenchGroups()

	for i := 0; i < 10; i++ {
		_ = g.Set(context.Background(), "exist-"+itoa(i), makeValue(defaultPayloadSize))
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, _ := g.Get(context.Background(), "nonexistent-key")
			sink = v
		}
	})
}

func BenchmarkGroupGet_SingleFlight(b *testing.B) {
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		time.Sleep(10 * time.Millisecond)
		return makeValue(defaultPayloadSize), nil
	})
	defer cleanupBenchGroups()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, err := g.Get(context.Background(), "hot-key")
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
	})
}

func BenchmarkGroupGet_SingleFlight_HighContention(b *testing.B) {
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		time.Sleep(50 * time.Millisecond)
		return makeValue(defaultPayloadSize), nil
	})
	defer cleanupBenchGroups()

	b.SetParallelism(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, err := g.Get(context.Background(), "contended-key")
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
	})
}

func BenchmarkStore_LRU_Sequential(b *testing.B) {
	benchStoreSequential(b, store.LRU)
}

func BenchmarkStore_LRU2_Sequential(b *testing.B) {
	benchStoreSequential(b, store.LRU2)
}

func benchStoreSequential(b *testing.B, ct store.CacheType) {
	opts := store.NewOptions()
	opts.MaxBytes = 1 << 30
	opts.CapPerBucket = 2048
	opts.Level2Cap = 2048
	s := store.NewStore(ct, opts)
	defer s.Close()

	keys := benchKeys("seq-", b.N)
	val := benchValue{data: makeValue(defaultPayloadSize)}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = s.Set(keys[i%len(keys)], val)
			s.Get(keys[i%len(keys)])
			i++
		}
	})
}

func BenchmarkStore_LRU_RandomAccess(b *testing.B) {
	benchStoreRandomAccess(b, store.LRU)
}

func BenchmarkStore_LRU2_RandomAccess(b *testing.B) {
	benchStoreRandomAccess(b, store.LRU2)
}

func benchStoreRandomAccess(b *testing.B, ct store.CacheType) {
	opts := store.NewOptions()
	opts.MaxBytes = 1 << 30
	opts.CapPerBucket = 2048
	opts.Level2Cap = 2048
	s := store.NewStore(ct, opts)
	defer s.Close()

	keys := benchKeys("rand-", 10000)
	for _, k := range keys {
		_ = s.Set(k, benchValue{data: makeValue(defaultPayloadSize)})
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idx := int(time.Now().UnixNano()) % len(keys)
			v, _ := s.Get(keys[idx])
			if v != nil {
				sink = v
			}
		}
	})
}

func BenchmarkStore_LRU_ParallelSet(b *testing.B) {
	benchStoreParallelSet(b, store.LRU)
}

func BenchmarkStore_LRU2_ParallelSet(b *testing.B) {
	benchStoreParallelSet(b, store.LRU2)
}

func benchStoreParallelSet(b *testing.B, ct store.CacheType) {
	opts := store.NewOptions()
	opts.MaxBytes = 1 << 30
	opts.CapPerBucket = 4096
	opts.Level2Cap = 4096
	s := store.NewStore(ct, opts)
	defer s.Close()

	val := benchValue{data: makeValue(defaultPayloadSize)}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var localIdx int
		for pb.Next() {
			key := "pset-" + itoa(localIdx)
			_ = s.Set(key, val)
			localIdx++
		}
	})
}

func BenchmarkStore_LRU_vs_LRU2_WriteRead_95_5(b *testing.B) {
	b.Run("LRU", func(b *testing.B) {
		benchStoreWriteReadRatio(b, store.LRU, 0.95)
	})
	b.Run("LRU2", func(b *testing.B) {
		benchStoreWriteReadRatio(b, store.LRU2, 0.95)
	})
}

func benchStoreWriteReadRatio(b *testing.B, ct store.CacheType, writeRatio float64) {
	opts := store.NewOptions()
	opts.MaxBytes = 1 << 30
	opts.CapPerBucket = 2048
	opts.Level2Cap = 2048
	s := store.NewStore(ct, opts)
	defer s.Close()

	baseKeys := benchKeys("wr-", 5000)
	for _, k := range baseKeys {
		_ = s.Set(k, benchValue{data: makeValue(defaultPayloadSize)})
	}

	val := benchValue{data: makeValue(defaultPayloadSize)}
	writeEvery := int(1.0 / (1.0 - writeRatio))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%writeEvery == 0 {
				_ = s.Set("wr-new-"+itoa(i), val)
			} else {
				s.Get(baseKeys[i%len(baseKeys)])
			}
			i++
		}
	})
}

func BenchmarkGroupWithExpiration(b *testing.B) {
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	}, reachcache.WithExpiration(100*time.Millisecond))
	defer cleanupBenchGroups()

	key := "ttl-key"
	_ = g.Set(context.Background(), key, makeValue(defaultPayloadSize))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, _ := g.Get(context.Background(), key)
			sink = v
		}
	})
}

func BenchmarkSingleFlight_NoContention(b *testing.B) {
	sf := &singleflight.Group{}
	val := makeValue(defaultPayloadSize)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, err := sf.Do("unique-key", func() (interface{}, error) {
				return val, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
	})
}

func BenchmarkSingleFlight_HighContention(b *testing.B) {
	sf := &singleflight.Group{}
	val := makeValue(defaultPayloadSize)
	b.SetParallelism(100)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, err := sf.Do("same-key", func() (interface{}, error) {
				return val, nil
			})
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
	})
}

func BenchmarkGroupGet_ConcurrentDifferentKeys(b *testing.B) {
	n := 1000
	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	})
	defer cleanupBenchGroups()

	keys := benchKeys("cdk-", n)
	for _, k := range keys {
		_ = g.Set(context.Background(), k, makeValue(defaultPayloadSize))
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			v, _ := g.Get(context.Background(), keys[i%n])
			sink = v
			i++
		}
	})
}

func BenchmarkGroupGet_BackfillRemote(b *testing.B) {
	peer := &benchPeer{
		getFunc: func(ctx context.Context, group, key string) ([]byte, error) {
			return makeValue(defaultPayloadSize), nil
		},
	}
	picker := &benchPeerPicker{
		pickFunc: func(key string) (reachcache.Peer, bool, bool) {
			return peer, true, false
		},
	}

	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	}, reachcache.WithPeers(picker))
	defer cleanupBenchGroups()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			v, err := g.Get(context.Background(), "remote-"+itoa(i))
			if err != nil {
				b.Fatal(err)
			}
			sink = v
			i++
		}
	})
}

func BenchmarkGroupGet_BackfillRemoteThenLocalHit(b *testing.B) {
	peer := &benchPeer{
		getFunc: func(ctx context.Context, group, key string) ([]byte, error) {
			return makeValue(defaultPayloadSize), nil
		},
	}
	picker := &benchPeerPicker{
		pickFunc: func(key string) (reachcache.Peer, bool, bool) {
			return peer, true, false
		},
	}

	g := setupBenchGroup(func(ctx context.Context, key string) ([]byte, error) {
		return makeValue(defaultPayloadSize), nil
	}, reachcache.WithPeers(picker))
	defer cleanupBenchGroups()

	key := "backfilled-key"
	_, _ = g.Get(context.Background(), key)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v, _ := g.Get(context.Background(), key)
			sink = v
		}
	})
}
