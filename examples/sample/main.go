package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	reachcache "github.com/vernmorn/reachcache"
)

func main() {
	// ── 环境变量（每次运行一个节点，靠这几个变量区分身份）──
	nodeID := getEnv("NODE_ID", "A")
	svcName := getEnv("SVC_NAME", "reach-cache")
	etcdAddr := getEnv("ETCD_ADDR", "127.0.0.1:2379")
	nodePort := getEnv("NODE_PORT", "8001")
	statsPort := getEnv("STATS_PORT", "18001")
	advertiseHost := getEnv("ADVERTISE_HOST", "") // 跨机器时设为本机 IP

	listenAddr := fmt.Sprintf(":%s", nodePort)

	advertiseAddr := advertiseHost
	if advertiseAddr == "" {
		hn, _ := os.Hostname()
		advertiseAddr = fmt.Sprintf("%s:%s", hn, nodePort)
	} else {
		advertiseAddr = fmt.Sprintf("%s:%s", advertiseHost, nodePort)
	}

	log.SetPrefix(fmt.Sprintf("[%s] ", nodeID))
	log.SetFlags(log.Ltime | log.Lmsgprefix)
	log.Printf("══════════════════════════════════════")
	log.Printf("  ID=%-4s  gRPC=%-6s  Stats=%-6s", nodeID, listenAddr, statsPort)
	log.Printf("  advertise=%s  etcd=%s", advertiseAddr, etcdAddr)
	log.Printf("══════════════════════════════════════")

	// ── 1. gRPC 服务端 + etcd 注册 ──
	server, err := reachcache.NewServer(listenAddr, svcName,
		reachcache.WithEtcdEndpoints([]string{etcdAddr}),
		reachcache.WithDialTimeout(5*time.Second),
		reachcache.WithStatsAddr(":"+statsPort),
		reachcache.WithAdvertiseAddr(advertiseAddr),
	)
	if err != nil {
		log.Fatalf("创建 Server 失败: %v", err)
	}

	// ── 2. 节点选择器（etcd 发现 + 一致性哈希）──
	picker, err := reachcache.NewClientPicker(
		advertiseAddr,
		reachcache.WithServiceName(svcName),
		reachcache.WithEtcdEndpointsForClientPicker([]string{etcdAddr}),
	)
	if err != nil {
		log.Fatalf("创建 ClientPicker 失败: %v", err)
	}

	// ── 3. 缓存组 ──
	group := reachcache.NewGroup(
		"demo",
		8<<20,
		reachcache.GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			log.Printf("★★★ 回源加载: key=%s ★★★", key)
			return fmt.Appendf(nil, "[%s] 回源数据 for %s", nodeID, key), nil
		}),
		reachcache.WithPeers(picker),
		reachcache.WithExpiration(3*time.Minute),
	)

	// ── 4. 启动 gRPC ──
	go func() {
		if err := server.Start(); err != nil {
			log.Fatalf("启动失败: %v", err)
		}
	}()

	// ── 5. 等待注册 & 发现其他节点 ──
	log.Printf("等待节点发现（1s）...")
	time.Sleep(15 * time.Second)
	picker.PrintPeers()

	// ═══════════════════════════════════════════
	//  演示：本地命中 → 一致性哈希 → gRPC 拉取 → 回填
	// ═══════════════════════════════════════════
	ctx := context.Background()
	ownKey := fmt.Sprintf("key_%s", nodeID)
	allKeys := []string{"key_A", "key_B", "key_C"}

	// 阶段1: 写入自身数据
	log.Printf("━━━ 阶段1: 写入自身数据 %s ━━━", ownKey)
	if err := group.Set(ctx, ownKey, fmt.Appendf(nil, "节点%s 专属数据", nodeID)); err != nil {
		log.Printf("✘ 写入失败: %v", err)
	} else {
		log.Printf("✔ 写入成功")
	}

	// 等待其他节点也完成 Set 和同步（避免时序竞争）
	time.Sleep(5 * time.Second)

	// 阶段2: 首轮查询 — 本地命中 / 远端拉取 / 回源加载
	log.Printf("━━━ 阶段2: 首轮查询（触发远端拉取 & 回填）━━━")
	for _, key := range allKeys {
		val, err := group.Get(ctx, key)
		if err != nil {
			log.Printf("  Get(%s) ✘ %v", key, err)
		} else {
			tag := "本地命中"
			if key == ownKey && strings.Contains(val.String(), "回源数据") {
				tag = "回源加载"
			} else if key != ownKey {
				tag = "远端拉取→本地回填"
			}
			log.Printf("  Get(%s) ✔ [%s] %s", key, tag, val.String())
		}
	}

	// 阶段3: 再次查询 — 验证回填后全部本地命中
	log.Printf("━━━ 阶段3: 再次查询（验证回填后全部本地命中）━━━")
	for _, key := range allKeys {
		val, _ := group.Get(ctx, key)
		log.Printf("  Get(%s) ✔ [本地命中(回填生效)] %s", key, val.String())
	}

	// 阶段4: 统计
	log.Printf("━━━ 阶段4: 统计 ━━━")
	stats := group.Stats()
	for k, v := range stats {
		log.Printf("  %-16s = %v", k, v)
	}

	log.Printf("══════════════════════════════════════")
	log.Printf("  节点 %s 就绪，保持运行", nodeID)
	log.Printf("  统计接口: http://localhost:%s/stats?group=demo", statsPort)
	log.Printf("══════════════════════════════════════")

	// 保持运行
	select {}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
