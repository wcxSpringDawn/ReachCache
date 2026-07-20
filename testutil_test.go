package reachcache

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/vernmorn/reachcache/consistenthash"
	proto "github.com/vernmorn/reachcache/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// mockPeer implements Peer with controllable behavior and channel-based async notification.
// setCalled / deleteCalled are buffered(1) channels — they receive a signal on the first call.
type mockPeer struct {
	t            testing.TB
	getFunc      func(ctx context.Context, group, key string) ([]byte, error)
	setFunc      func(ctx context.Context, group, key string, value []byte) error
	deleteFunc   func(ctx context.Context, group, key string) (bool, error)
	closeFunc    func() error
	setCalled    chan struct{}
	deleteCalled chan struct{}
}

func (m *mockPeer) Get(group, key string) ([]byte, error) {
	if m.getFunc != nil {
		return m.getFunc(context.Background(), group, key)
	}
	return []byte("mock-data"), nil
}

func (m *mockPeer) Set(ctx context.Context, group, key string, value []byte) error {
	select {
	case m.setCalled <- struct{}{}:
	default:
	}
	if m.setFunc != nil {
		return m.setFunc(ctx, group, key, value)
	}
	return nil
}

func (m *mockPeer) Delete(ctx context.Context, group, key string) (bool, error) {
	select {
	case m.deleteCalled <- struct{}{}:
	default:
	}
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, group, key)
	}
	return true, nil
}

func (m *mockPeer) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

// mockPeerPicker implements PeerPicker with controllable routing.
type mockPeerPicker struct {
	pickFunc  func(key string) (peer Peer, ok bool, self bool)
	closeFunc func() error
}

func (m *mockPeerPicker) PickPeer(key string) (Peer, bool, bool) {
	if m.pickFunc != nil {
		return m.pickFunc(key)
	}
	return nil, false, false
}

func (m *mockPeerPicker) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

// startTestGRPCServer creates an in-process gRPC server on a random port (127.0.0.1:0).
// It registers ReachCache and health services but does NOT connect to etcd.
// Groups must be registered separately via NewGroup before making requests.
func startTestGRPCServer(t testing.TB, svcName string) (addr string, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv := &Server{
		addr:       lis.Addr().String(),
		svcName:    svcName,
		grpcServer: grpcServer,
		stopCh:     make(chan error),
		opts:       &ServerOptions{},
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

// newTestClient creates a minimal gRPC Client connecting to addr.
// It does NOT use etcd — suitable for in-process testing.
func newTestClient(t testing.TB, addr string) *Client {
	t.Helper()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 100 * time.Millisecond,
		}),
	)
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}

	return &Client{
		addr:    addr,
		conn:    conn,
		grpcCli: proto.NewReachCacheClient(conn),
	}
}

// newTestPicker creates a ClientPicker without etcd, suitable for testing PickPeer.
func newTestPicker(selfAddr string, peers map[string]*Client) *ClientPicker {
	p := &ClientPicker{
		selfAddr: selfAddr,
		clients:  make(map[string]*Client),
		consHash: consistenthash.New(),
	}
	for addr, client := range peers {
		p.consHash.Add(addr)
		p.clients[addr] = client
	}
	return p
}
