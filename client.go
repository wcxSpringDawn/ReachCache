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
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	proto "github.com/wcxSpringDawn/ReachCache/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Client 是 Peer 接口的 gRPC 实现，封装了到远端缓存节点的连接和 RPC 调用。
// 通过 grpc.NewClient 建立连接（非阻塞，首次 RPC 时按需建立），
// 支持 TLS 加密和 Token 认证。
type Client struct {
	addr        string
	svcName     string
	etcdCli     *clientv3.Client
	ownsEtcdCli bool
	conn        *grpc.ClientConn
	grpcCli     proto.ReachCacheClient
}

// ClientOptions 控制 gRPC 客户端的连接行为。
type ClientOptions struct {
	TLS                bool
	CACertFile         string // CA证书文件，用于验证服务端身份
	ServerName         string // 服务端名称（需与证书SAN匹配）
	InsecureSkipVerify bool   // 跳过服务端证书验证（仅测试环境）
	AuthToken          string // 节点间认证令牌，非空时通过PerRPCCredentials自动携带
}

// ClientOption 定义客户端配置项
type ClientOption func(*ClientOptions)

// WithClientTLS 启用TLS连接，可选传入CA证书文件和ServerName。
func WithClientTLS(caCertFile, serverName string, insecureSkipVerify bool) ClientOption {
	return func(o *ClientOptions) {
		o.TLS = true
		o.CACertFile = caCertFile
		o.ServerName = serverName
		o.InsecureSkipVerify = insecureSkipVerify
	}
}

// WithClientAuthToken 配置认证令牌。设置后客户端在每次RPC调用时自动携带该令牌，
// 以通过服务端的token拦截器校验。测试环境可省略。
func WithClientAuthToken(token string) ClientOption {
	return func(o *ClientOptions) {
		o.AuthToken = token
	}
}

var _ Peer = (*Client)(nil)

// NewClient 创建到远端缓存节点的 gRPC 客户端。
// 若 etcdCli 为 nil 则自动创建一个本地 etcd 客户端。
// 支持通过 ClientOption 配置 TLS 加密和 Token 认证。
func NewClient(addr string, svcName string, etcdCli *clientv3.Client, opts ...ClientOption) (*Client, error) {
	var err error
	ownsEtcdCli := false
	if etcdCli == nil {
		etcdCli, err = clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create etcd client: %v", err)
		}
		ownsEtcdCli = true
	}

	clientOpts := ClientOptions{}
	for _, opt := range opts {
		opt(&clientOpts)
	}

	transportCreds := insecure.NewCredentials()
	if clientOpts.TLS {
		creds, err := loadClientTLSCredentials(clientOpts)
		if err != nil {
			if ownsEtcdCli {
				_ = etcdCli.Close()
			}
			return nil, fmt.Errorf("failed to load client TLS credentials: %v", err)
		}
		transportCreds = creds
	}

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithConnectParams(grpc.ConnectParams{
			MinConnectTimeout: 10 * time.Second,
		}),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	// 若配置了认证令牌，通过PerRPCCredentials自动在每次RPC的metadata中携带
	if clientOpts.AuthToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&tokenCreds{token: clientOpts.AuthToken}))
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		if ownsEtcdCli {
			_ = etcdCli.Close()
		}
		return nil, fmt.Errorf("failed to dial server: %v", err)
	}

	grpcClient := proto.NewReachCacheClient(conn)

	client := &Client{
		addr:        addr,
		svcName:     svcName,
		etcdCli:     etcdCli,
		ownsEtcdCli: ownsEtcdCli,
		conn:        conn,
		grpcCli:     grpcClient,
	}

	return client, nil
}

func loadClientTLSCredentials(opts ClientOptions) (credentials.TransportCredentials, error) {
	tlsConfig := &tls.Config{
		ServerName:         opts.ServerName,
		InsecureSkipVerify: opts.InsecureSkipVerify,
		MinVersion:         tls.VersionTLS12,
	}

	if opts.CACertFile != "" {
		pemServerCA, err := os.ReadFile(opts.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read server CA file: %w", err)
		}
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(pemServerCA) {
			return nil, fmt.Errorf("failed to add server CA cert")
		}
		tlsConfig.RootCAs = certPool
	}

	return credentials.NewTLS(tlsConfig), nil
}

// tokenCreds 实现 credentials.PerRPCCredentials 接口，在每次RPC调用时自动注入认证令牌。
type tokenCreds struct {
	token string
}

func (t *tokenCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + t.token,
	}, nil
}

func (t *tokenCreds) RequireTransportSecurity() bool {
	return false // token本身不要求TLS，生产环境应配合TLS使用
}

// Get 实现 Peer 接口。通过 gRPC 向远端节点请求缓存数据（3 秒超时）。
func (c *Client) Get(group, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := c.grpcCli.Get(ctx, &proto.GetRequest{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get value from reachcache: %v", err)
	}

	return resp.GetValue(), nil
}

// Delete 实现 Peer 接口。向远端节点发送删除请求。
func (c *Client) Delete(ctx context.Context, group, key string) (bool, error) {
	resp, err := c.grpcCli.Delete(ctx, &proto.DeleteRequest{
		Group: group,
		Key:   key,
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete value from reachcache: %v", err)
	}

	if resp.Code != 0 {
		return false, fmt.Errorf("delete failed: %s", resp.Msg)
	}

	return true, nil
}

// Set 实现 Peer 接口。向远端节点发送写入请求，超时和取消由上层 ctx 控制。
func (c *Client) Set(ctx context.Context, group string, key string, value []byte) error {
	resp, err := c.grpcCli.Set(ctx, &proto.SetRequest{
		Group: group,
		Key:   key,
		Value: value,
	})

	if err != nil {
		return fmt.Errorf("failed to set value to reachcache: %v", err)
	}

	if resp.Code != 0 {
		return fmt.Errorf("set failed: %s", resp.Msg)
	}

	logrus.Infof("grpc set request resp: %+v", resp)

	return nil
}

// Close 关闭 gRPC 连接和 etcd 客户端（仅当 Client 拥有所有权时关闭 etcd）。
func (c *Client) Close() error {
	var closeErr error
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			closeErr = err
		}
	}
	if c.ownsEtcdCli && c.etcdCli != nil {
		if err := c.etcdCli.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}
