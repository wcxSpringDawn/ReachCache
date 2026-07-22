package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	reachcache "github.com/vernmorn/reachcache"
)

func main() {
	addr := flag.String("addr", ":50051", "gRPC server listen address")
	svcName := flag.String("svc", "reach-cache", "service name for etcd registration")
	etcdEndpoints := flag.String("etcd", "127.0.0.1:2379", "comma-separated etcd endpoints")
	advertiseAddr := flag.String("advertise", "", "advertise address for etcd (default: same as -addr)")
	cacheSize := flag.Int64("cache", 512<<20, "per-group cache max bytes (default 512MB)")
	groupCount := flag.Int("groups", 1, "number of benchmark groups to pre-register")
	statsAddr := flag.String("stats", ":18001", "HTTP stats endpoint address (empty to disable)")
	token := flag.String("token", "", "node auth token (empty = no auth)")
	certFile := flag.String("tls-cert", "", "TLS certificate file path")
	keyFile := flag.String("tls-key", "", "TLS key file path")
	flag.Parse()

	endpoints := splitList(*etcdEndpoints)

	for i := range *groupCount {
		name := fmt.Sprintf("bench-group-%d", i)
		reachcache.NewGroup(name, *cacheSize,
			reachcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
				return []byte("default-value"), nil
			}),
		)
		log.Printf("registered group: %s (cache: %d bytes)", name, *cacheSize)
	}

	var serverOpts []reachcache.ServerOption
	serverOpts = append(serverOpts,
		reachcache.WithEtcdEndpoints(endpoints),
		reachcache.WithDialTimeout(5*time.Second),
	)

	if *statsAddr != "" {
		serverOpts = append(serverOpts, reachcache.WithStatsAddr(*statsAddr))
	}
	if *advertiseAddr != "" {
		serverOpts = append(serverOpts, reachcache.WithAdvertiseAddr(*advertiseAddr))
	}
	if *token != "" {
		serverOpts = append(serverOpts, reachcache.WithServerAuthToken(*token))
	}
	if *certFile != "" && *keyFile != "" {
		serverOpts = append(serverOpts, reachcache.WithTLS(*certFile, *keyFile))
	}

	srv, err := reachcache.NewServer(*addr, *svcName, serverOpts...)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("shutting down...")
		srv.Stop()
	}()

	log.Printf("cloud benchmark node starting on %s (svc=%s, etcd=%v, groups=%d)",
		*addr, *svcName, endpoints, *groupCount)
	if err := srv.Start(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if i > start {
				result = append(result, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}
