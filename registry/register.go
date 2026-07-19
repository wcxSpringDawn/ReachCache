/*
Copyright 2026 Wang Chunxiao (vernmorn)

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

package registry

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Config 定义etcd客户端配置
type Config struct {
	Endpoints   []string      // 集群地址
	DialTimeout time.Duration // 连接超时时间
}

// DefaultConfig 提供默认配置
var DefaultConfig = &Config{
	Endpoints:   []string{"localhost:2379"},
	DialTimeout: 5 * time.Second,
}

// Register 注册服务到etcd。
// listenAddr 是本地监听地址（可为":8001"），advertiseAddr 是注册到etcd供其他节点访问的地址（格式"ip:port"）。
func Register(svcName, listenAddr, advertiseAddr string, endpoints []string, dialTimeout time.Duration, stopCh <-chan error) error {
	if len(endpoints) == 0 {
		endpoints = DefaultConfig.Endpoints
	}
	if dialTimeout <= 0 {
		dialTimeout = DefaultConfig.DialTimeout
	}

	requestTimeout := dialTimeout
	if requestTimeout <= 0 {
		requestTimeout = DefaultConfig.DialTimeout
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %v", err)
	}

	addr := advertiseAddr
	if addr == "" {
		addr = listenAddr
		localIP, err := getLocalIP()
		if err != nil {
			cli.Close()
			return fmt.Errorf("failed to get local IP: %v", err)
		}
		if len(addr) > 0 && addr[0] == ':' {
			addr = fmt.Sprintf("%s%s", localIP, addr)
		}
	}

	// 创建租约
	grantCtx, grantCancel := context.WithTimeout(context.Background(), requestTimeout)
	lease, err := cli.Grant(grantCtx, 10) // 增加租约时间到10秒
	grantCancel()
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to create lease: %v", err)
	}

	// 注册服务，使用完整的key路径
	key := fmt.Sprintf("/services/%s/%s", svcName, addr)
	putCtx, putCancel := context.WithTimeout(context.Background(), requestTimeout)
	_, err = cli.Put(putCtx, key, addr, clientv3.WithLease(lease.ID))
	putCancel()
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to put key-value to etcd: %v", err)
	}

	// 保持租约
	keepAliveCh, err := cli.KeepAlive(context.Background(), lease.ID)
	if err != nil {
		cli.Close()
		return fmt.Errorf("failed to keep lease alive: %v", err)
	}

	// 处理租约续期和服务注销
	go func() {
		defer cli.Close()
		for {
			select {
			case <-stopCh:
				// 服务注销，撤销租约
				ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
				cli.Revoke(ctx, lease.ID)
				cancel()
				return
			case resp, ok := <-keepAliveCh:
				if !ok {
					logrus.Warn("keep alive channel closed")
					return
				}
				logrus.Debugf("successfully renewed lease: %d", resp.ID)
			}
		}
	}()

	logrus.Infof("Service registered: %s at %s", svcName, addr)
	return nil
}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}

	return "", fmt.Errorf("no valid local IP found")
}
