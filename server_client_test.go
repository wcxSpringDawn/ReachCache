package reachcache

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	proto "github.com/vernmorn/reachcache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// startTestAuthGRPCServer creates an in-process gRPC server with optional token auth.
func startTestAuthGRPCServer(t testing.TB, svcName, token string) (addr string, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	var serverOpts []grpc.ServerOption
	if token != "" {
		serverOpts = append(serverOpts, grpc.UnaryInterceptor(tokenInterceptor(token)))
	}

	grpcServer := grpc.NewServer(serverOpts...)
	srv := &Server{
		addr:       lis.Addr().String(),
		svcName:    svcName,
		grpcServer: grpcServer,
		stopCh:     make(chan error),
		opts:       &ServerOptions{AuthToken: token},
	}

	proto.RegisterReachCacheServer(grpcServer, srv)

	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("gRPC server stopped: %v", err)
		}
	}()

	return srv.addr, func() {
		grpcServer.GracefulStop()
		lis.Close()
	}
}

// newTestAuthClient creates a gRPC Client with token credentials.
func newTestAuthClient(t testing.TB, addr, token string) *Client {
	t.Helper()

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 100 * time.Millisecond,
		}),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if token != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&tokenCreds{token: token}))
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}

	return &Client{
		addr:    addr,
		conn:    conn,
		grpcCli: proto.NewReachCacheClient(conn),
	}
}

// ======================== 基本 Get/Set/Delete RPC ========================

func TestServerClient_Get(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-get-rpc"
	addr, stop := startTestGRPCServer(t, svcName)
	defer stop()

	// 在 Server 侧创建 Group 并写入数据
	g := newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		t.Fatal("getter should not be called")
		return nil, nil
	}))
	g.Set(context.Background(), "mykey", []byte("myvalue"))

	client := newTestClient(t, addr)
	data, err := client.Get("g1", "mykey")
	if err != nil {
		t.Fatalf("client Get failed: %v", err)
	}
	if string(data) != "myvalue" {
		t.Fatalf("expected 'myvalue', got '%s'", string(data))
	}
}

func TestServerClient_Set(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-set-rpc"
	addr, stop := startTestGRPCServer(t, svcName)
	defer stop()

	g := newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("dummy"), nil
	}))

	client := newTestClient(t, addr)
	if err := client.Set(context.Background(), "g1", "mykey", []byte("set-via-rpc")); err != nil {
		t.Fatalf("client Set failed: %v", err)
	}

	// 验证数据落入 Server 侧本地缓存
	v, err := g.Get(context.Background(), "mykey")
	if err != nil {
		t.Fatalf("local Get after remote Set failed: %v", err)
	}
	if string(v.ByteSlice()) != "set-via-rpc" {
		t.Fatalf("expected 'set-via-rpc', got '%s'", string(v.ByteSlice()))
	}
}

func TestServerClient_Delete(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-del-rpc"
	addr, stop := startTestGRPCServer(t, svcName)
	defer stop()

	newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("dummy"), nil
	})).Set(context.Background(), "mykey", []byte("todelete"))

	client := newTestClient(t, addr)
	ok, err := client.Delete(context.Background(), "g1", "mykey")
	if err != nil {
		t.Fatalf("client Delete failed: %v", err)
	}
	if !ok {
		t.Fatal("Delete should return true")
	}

	// 验证 Server 侧 key 已删除
	if _, ok := GetGroup("g1").mainCache.Get(context.Background(), "mykey"); ok {
		t.Fatal("key should be deleted from server cache")
	}
}

// ======================== 错误码 ========================

func TestServerClient_Get_GroupNotFound(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-no-group"
	addr, stop := startTestGRPCServer(t, svcName)
	defer stop()

	client := newTestClient(t, addr)
	_, err := client.Get("nonexistent-group", "mykey")
	if err == nil {
		t.Fatal("expected error for non-existent group")
	}
}

func TestServerClient_Get_GetterFailure(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-getter-err"
	addr, stop := startTestGRPCServer(t, svcName)
	defer stop()

	newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return nil, errors.New("getter failed")
	}))

	client := newTestClient(t, addr)
	_, err := client.Get("g1", "any-key")
	if err == nil {
		t.Fatal("expected error when getter fails")
	}
}

// ======================== Token 认证 ========================

func TestServerClient_TokenAuth_Valid(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-auth-valid"
	token := "my-secret-token"
	addr, stop := startTestAuthGRPCServer(t, svcName, token)
	defer stop()

	newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("auth-data"), nil
	})).Set(context.Background(), "k", []byte("auth-data"))

	client := newTestAuthClient(t, addr, token)
	data, err := client.Get("g1", "k")
	if err != nil {
		t.Fatalf("authenticated Get failed: %v", err)
	}
	if string(data) != "auth-data" {
		t.Fatalf("expected 'auth-data', got '%s'", string(data))
	}
}

func TestServerClient_TokenAuth_Invalid(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-auth-invalid"
	addr, stop := startTestAuthGRPCServer(t, svcName, "server-token")
	defer stop()

	newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("x"), nil
	})).Set(context.Background(), "k", []byte("x"))

	client := newTestAuthClient(t, addr, "wrong-token")
	_, err := client.Get("g1", "k")
	if err == nil {
		t.Fatal("expected PermissionDenied for invalid token")
	}
}

func TestServerClient_TokenAuth_Missing(t *testing.T) {
	defer cleanupGroups()

	svcName := "test-auth-missing"
	addr, stop := startTestAuthGRPCServer(t, svcName, "server-token")
	defer stop()

	newTestGroup("g1", GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
		return []byte("x"), nil
	})).Set(context.Background(), "k", []byte("x"))

	// 不带 token 的 client
	client := newTestClient(t, addr)
	_, err := client.Get("g1", "k")
	if err == nil {
		t.Fatal("expected PermissionDenied for missing token")
	}
}

// ======================== Health Check ========================

func TestServer_HealthCheck(t *testing.T) {
	svcName := "test-health"
	addr, stop := startTestGRPCServer(t, svcName)
	defer stop()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 100 * time.Millisecond,
		}),
	)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	healthClient := healthpb.NewHealthClient(conn)
	resp, err := healthClient.Check(context.Background(), &healthpb.HealthCheckRequest{Service: svcName})
	if err != nil {
		t.Fatalf("health check failed: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING, got %v", resp.Status)
	}
}

// ======================== from_peer 注入 ========================

func TestServer_Set_FromPeerInjected(t *testing.T) {
	defer cleanupGroups()

	// 注册一个 mock peer，若 syncToPeers 被调用则 setCalled 会收到信号
	mp := &mockPeer{setCalled: make(chan struct{}, 1)}
	mpp := &mockPeerPicker{
		pickFunc: func(key string) (Peer, bool, bool) {
			return mp, true, false
		},
	}

	svcName := "test-from-peer-mark"
	addr, stop := startTestGRPCServer(t, svcName)
	defer stop()

	newTestGroup("g1",
		GetterFunc(func(ctx context.Context, key string) ([]byte, error) {
			return []byte("dummy"), nil
		}),
		WithPeers(mpp),
	)

	// 通过 gRPC 调用 Set
	client := newTestClient(t, addr)
	if err := client.Set(context.Background(), "g1", "k", []byte("v")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Server 应注入 from_peer，从而使 syncToPeers 被抑制
	select {
	case <-mp.setCalled:
		t.Fatal("syncToPeers should NOT be called — server should inject from_peer")
	case <-time.After(100 * time.Millisecond):
	}
}
