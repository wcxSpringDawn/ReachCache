/*
Copyright 2026 wcxSpringDawn

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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	proto "github.com/vernmorn/reachcache/proto"
	registry "github.com/vernmorn/reachcache/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Server 是 gRPC 缓存服务端，实现了 proto.ReachCacheServer 接口。
// 负责监听入站 gRPC 连接、解析请求、路由到对应 Group 并返回结果。
// Group 查找通过全局注册表 GetGroup(name) 完成，Server 本身不维护独立的 Group 映射。
type Server struct {
	proto.UnimplementedReachCacheServer                  // gRPC自动生成的基类，提供接口的默认实现
	addr                                string           // 服务监听地址，格式为"ip:port"
	svcName                             string           // 服务名称，用于在服务注册中心标识服务
	grpcServer                          *grpc.Server     // gRPC服务器实例
	etcdCli                             *clientv3.Client // etcd客户端，用于服务注册
	stopCh                              chan error       // 停止信号通道，用于服务优雅停止
	opts                                *ServerOptions   // 服务器配置选项
	statsHTTPServer                     *http.Server     // 可选HTTP统计服务，用于外部采集Group统计信息
}

// ServerOptions 是 gRPC 服务端的配置项。
type ServerOptions struct {
	EtcdEndpoints []string      // etcd端点
	DialTimeout   time.Duration // 连接超时
	MaxMsgSize    int           // 最大消息大小
	StatsAddr     string        // 统计接口监听地址，空字符串表示不启用
	AdvertiseAddr string        // 注册到etcd并供其他节点访问的地址，格式为"ip:port"
	TLS           bool          // 是否启用TLS
	CertFile      string        // 服务端证书文件（PEM格式）
	KeyFile       string        // 服务端密钥文件（PEM格式）
	AuthToken     string        // 节点间认证令牌，为空则不启用认证（仅测试环境）
}

// DefaultServerOptions 返回服务端默认配置：etcd 本地端点、5 秒超时、4MB 消息限制。
var DefaultServerOptions = &ServerOptions{
	EtcdEndpoints: []string{"localhost:2379"},
	DialTimeout:   5 * time.Second,
	MaxMsgSize:    4 << 20, // 4MB
}

// ServerOption 定义选项函数类型
type ServerOption func(*ServerOptions)

// WithEtcdEndpoints 设置etcd端点
func WithEtcdEndpoints(endpoints []string) ServerOption {
	return func(o *ServerOptions) {
		o.EtcdEndpoints = endpoints
	}
}

// WithDialTimeout 设置连接超时
func WithDialTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.DialTimeout = timeout
	}
}

// WithStatsAddr 设置统计接口地址，例如":18001"或"127.0.0.1:18001"
func WithStatsAddr(addr string) ServerOption {
	return func(o *ServerOptions) {
		o.StatsAddr = addr
	}
}

// WithAdvertiseAddr 设置注册到etcd的节点地址（用于其他节点访问）
func WithAdvertiseAddr(addr string) ServerOption {
	return func(o *ServerOptions) {
		o.AdvertiseAddr = addr
	}
}

// WithTLS 配置服务端TLS证书。若需启用TLS加密通信（推荐生产环境使用），传入PEM格式的证书和密钥文件路径。
func WithTLS(certFile, keyFile string) ServerOption {
	return func(o *ServerOptions) {
		o.TLS = true
		o.CertFile = certFile
		o.KeyFile = keyFile
	}
}

// WithServerAuthToken 配置节点间认证令牌。设置后，gRPC服务端只接受携带相同令牌的客户端连接，
// 从而阻止非缓存节点的外部程序访问。测试环境可省略此选项。
func WithServerAuthToken(token string) ServerOption {
	return func(o *ServerOptions) {
		o.AuthToken = token
	}
}

// NewServer 创建 gRPC 服务端实例。注册 ReachCache 服务、健康检查服务，
// 并配置可选的 TLS 加密和 Token 认证拦截器。
func NewServer(addr, svcName string, opts ...ServerOption) (*Server, error) {
	options := *DefaultServerOptions
	options.EtcdEndpoints = append([]string(nil), DefaultServerOptions.EtcdEndpoints...)
	for _, opt := range opts {
		opt(&options)
	}

	// 创建etcd客户端
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   options.EtcdEndpoints,
		DialTimeout: options.DialTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	// 创建gRPC服务器
	var serverOpts []grpc.ServerOption
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxMsgSize))

	// 若配置了认证令牌，注册拦截器以拒绝未授权的客户端连接
	if options.AuthToken != "" {
		serverOpts = append(serverOpts, grpc.UnaryInterceptor(tokenInterceptor(options.AuthToken)))
	}

	if options.TLS {
		creds, err := loadTLSCredentials(options.CertFile, options.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}

	srv := &Server{
		addr:       addr,
		svcName:    svcName,
		grpcServer: grpc.NewServer(serverOpts...),
		etcdCli:    etcdCli,
		stopCh:     make(chan error),
		opts:       &options,
	}

	// 注册服务
	proto.RegisterReachCacheServer(srv.grpcServer, srv)

	// 注册健康检查服务
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(srv.grpcServer, healthServer)
	healthServer.SetServingStatus(svcName, healthpb.HealthCheckResponse_SERVING)

	return srv, nil
}

// loadTLSCredentials 加载服务端TLS凭据（单向认证：客户端验证服务器身份）。
func loadTLSCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server certificate: %w", err)
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

// tokenInterceptor 创建gRPC一元拦截器，校验请求是否携带正确的认证令牌。
// 若令牌为空（测试模式）或请求携带的"authorization" metadata 为 "Bearer <token>"，则放行；否则返回 PermissionDenied。
func tokenInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// 跳过健康检查服务的认证
		if info.FullMethod == "/grpc.health.v1.Health/Check" {
			return handler(ctx, req)
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.PermissionDenied, "missing metadata")
		}
		authHeader := md.Get("authorization")
		if len(authHeader) == 0 || authHeader[0] != "Bearer "+token {
			return nil, status.Error(codes.PermissionDenied, "invalid or missing token")
		}
		return handler(ctx, req)
	}
}

// Start 启动 gRPC 服务：创建 TCP 监听 → etcd 服务注册（后台 goroutine）→ 阻塞 Serve。
// 若配置了 StatsAddr，同时启动 HTTP 统计接口。
func (s *Server) Start() error {
	// 启动gRPC服务器
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 注册到etcd
	go func() {
		if err := registry.Register(s.svcName, s.addr, s.opts.AdvertiseAddr, s.opts.EtcdEndpoints, s.opts.DialTimeout, s.stopCh); err != nil {
			logrus.Errorf("failed to register service: %v", err)
			return
		}
	}()

	logrus.Infof("Server starting at %s", s.addr)
	if s.opts != nil && s.opts.StatsAddr != "" {
		s.startStatsHTTPServer()
	}
	return s.grpcServer.Serve(lis)
}

// Stop 优雅停止服务：通知 etcd 注销 → 关闭统计 HTTP → GracefulStop gRPC → 关闭 etcd 连接。
func (s *Server) Stop() {
	close(s.stopCh)
	if s.statsHTTPServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = s.statsHTTPServer.Shutdown(ctx)
		cancel()
	}
	s.grpcServer.GracefulStop()
	if s.etcdCli != nil {
		s.etcdCli.Close()
	}
}

// Get 实现服务端Get方法
func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	// 根据请求中的组名找到对应的缓存组
	group := GetGroup(req.Group)
	if group == nil {
		err := fmt.Errorf("group %s not found", req.Group)
		return &proto.GetResponse{Code: 1, Msg: err.Error(), Value: []byte{}}, err
	}

	// 调用缓存组的Get方法获取数据
	view, err := group.Get(ctx, req.Key)
	if err != nil {
		return &proto.GetResponse{Code: 2, Msg: err.Error(), Value: []byte{}}, err
	}

	return &proto.GetResponse{Code: 0, Msg: "Get Success", Value: view.ByteSlice()}, nil
}

// Set 实现服务端Set方法
func (s *Server) Set(ctx context.Context, req *proto.SetRequest) (*proto.SetResponse, error) {
	group := GetGroup(req.Group)
	if group == nil {
		err := fmt.Errorf("group %s not found", req.Group)
		return &proto.SetResponse{Code: 1, Msg: err.Error()}, err
	}

	// 所有经由gRPC到达的Set请求均被视为来自对等节点的同步操作。
	// 由于token拦截器已确保只有缓存节点可建立gRPC连接，这里的"一刀切"是安全的。
	ctx = context.WithValue(ctx, "from_peer", true)

	if err := group.Set(ctx, req.Key, req.Value); err != nil {
		if err == ErrKeyRequired {
			return &proto.SetResponse{Code: 3, Msg: err.Error()}, err
		}
		if err == ErrValueRequired {
			return &proto.SetResponse{Code: 3, Msg: err.Error()}, err
		}
		if err == ErrGroupClosed {
			return &proto.SetResponse{Code: 3, Msg: err.Error()}, err
		}
		return &proto.SetResponse{Code: 3, Msg: err.Error()}, err
	}

	return &proto.SetResponse{Code: 0, Msg: "Set Success"}, nil
}

// Delete 实现服务端Delete方法
func (s *Server) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	group := GetGroup(req.Group)
	if group == nil {
		err := fmt.Errorf("group %s not found", req.Group)
		return &proto.DeleteResponse{Code: 1, Msg: err.Error()}, err
	}

	// 与Set一致：经由gRPC到达即视为peer同步，由token拦截器保证只有缓存节点可连接
	ctx = context.WithValue(ctx, "from_peer", true)

	err := group.Delete(ctx, req.Key)
	if err != nil {
		if err == ErrKeyRequired {
			return &proto.DeleteResponse{Code: 3, Msg: err.Error()}, err
		}
		if err == ErrGroupClosed {
			return &proto.DeleteResponse{Code: 3, Msg: err.Error()}, err
		}
		return &proto.DeleteResponse{Code: 3, Msg: err.Error()}, err
	}
	return &proto.DeleteResponse{Code: 0, Msg: "Delete Success"}, err
}

func (s *Server) startStatsHTTPServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")

		groupName := r.URL.Query().Get("group")
		if groupName == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "missing query parameter: group",
			})
			return
		}

		g := GetGroup(groupName)
		if g == nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error": fmt.Sprintf("group %s not found", groupName),
			})
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"node":    s.addr,
			"group":   groupName,
			"stats":   g.Stats(),
			"ts_unix": time.Now().Unix(),
		})
	})

	mux.HandleFunc("/stats/all", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		groups := ListGroups()
		all := make(map[string]interface{}, len(groups))
		for _, name := range groups {
			if g := GetGroup(name); g != nil {
				all[name] = g.Stats()
			}
		}

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"node":    s.addr,
			"groups":  all,
			"count":   len(all),
			"ts_unix": time.Now().Unix(),
		})
	})

	s.statsHTTPServer = &http.Server{
		Addr:    s.opts.StatsAddr,
		Handler: mux,
	}

	go func() {
		logrus.Infof("Stats HTTP server starting at %s", s.opts.StatsAddr)
		if err := s.statsHTTPServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Errorf("stats HTTP server error: %v", err)
		}
	}()
}
