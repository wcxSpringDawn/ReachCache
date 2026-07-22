# 分布式示例：短链接服务

演示 ReachCache 在**3 节点**分布式部署下的缓存分片与 gRPC 通信。

## 架构

```
                     etcd (:2379)
                    /     |     \
                   ▼      ▼      ▼
              Node A   Node B   Node C
             :50051   :50052   :50053
             :8081    :8082    :8083

            共享 SQLite 文件 (shortlink.db)
```

每个 Node 包含：

- **HTTP** — 对外提供缩短/跳转 API
- **gRPC** — 节点间缓存通信（ReachCache 内置）
- **Group** — `shortlink` 组，一致性哈希分片
- **Getter** — SQLite 查询（回源兜底）

### 短链请求流程

```text
POST /shorten?url=...  →  生成短码  →  写 SQLite + DB
                           →  group.Set(code, url)
                           →  本地缓存写入
                           →  异步 syncToPeers（一致性哈希路由到归属节点）

GET /:code  →  group.Get(code)
               ├── 本地命中 → 301 跳转
               ├── 远端命中 → gRPC 转发给归属节点 → 301 跳转
               └── Miss → Getter → SQLite 查询 → 回写 → 301 跳转
```

## 前置条件

- Go 1.26+
- etcd 服务（Docker 或本地安装）

## 运行

### 方式一：一键脚本

```bash
cd examples/shortlink

# Linux
bash start.sh

# Windows (PowerShell)
.\start.ps1
```

### 方式二：手动分别启动

```bash
cd examples/shortlink
ETCD_ENDPOINTS=127.0.0.1:2379
go run . -name node-a -addr :50051 -http :8081 -advertise 127.0.0.1:50051

# 新开终端
ETCD_ENDPOINTS=127.0.0.1:2379
go run . -name node-b -addr :50052 -http :8082 -advertise 127.0.0.1:50052

# 新开终端
ETCD_ENDPOINTS=127.0.0.1:2379
go run . -name node-c -addr :50053 -http :8083 -advertise 127.0.0.1:50053
```

### 通过环境变量配置 etcd

所有方式都支持通过 `ETCD_ENDPOINTS` 环境变量指定 etcd 地址：

```bash
# Linux
ETCD_ENDPOINTS=192.168.1.100:2379 bash start.sh

# Windows (PowerShell)
$env:ETCD_ENDPOINTS="192.168.1.100:2379"
.\start.ps1
```

## 演示

### 1. 生成短链

```bash
# Linux
curl -X POST 'http://localhost:8081/shorten?url=https://www.baidu.com'

# Windows (PowerShell)
Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/shorten?url=https://www.baidu.com'
```

返回：

```json
{
  "code": "1LOhlPyWMyJ",
  "url": "https://www.baidu.com",
  "short_url": "http://localhost:8081/1LOhlPyWMyJ"
}
```

### 2. 跨节点跳转

短码 `1LOhlPyWMyJ` 按一致性哈希分配到其中一个节点。从 **Node B** 访问：

```bash
# Linux
curl -v http://localhost:8082/1LOhlPyWMyJ

# Windows (PowerShell)
Invoke-WebRequest -Uri http://localhost:8082/1LOhlPyWMyJ
```

会触发：

1. Node B 本地 Cache Miss
2. 一致性哈希算出 key 归属（假设 Node C）
3. Node B 通过 gRPC 向 Node C:50053 发起 Get 请求
4. Node C 查询本地缓存（或 SQLite）返回原 URL
5. Node B 回写本地缓存，301 跳转到目标地址

再次访问则直接 Node B 本地命中。

### 3. 重复生成同一 URL

返回已有的短码，不会重复插入：

```bash
# Linux
curl -X POST 'http://localhost:8081/shorten?url=https://www.baidu.com'

# Windows (PowerShell)
Invoke-RestMethod -Method Post -Uri 'http://localhost:8081/shorten?url=https://www.baidu.com'
```

### 4. 缓存统计

```bash
# Linux
curl http://localhost:8083/stats

# Windows (PowerShell)
Invoke-RestMethod -Uri http://localhost:8083/stats
```

## 缓存配置

| 参数     | 值            |
| -------- | ------------- |
| Group 名 | `shortlink`   |
| 容量     | 32MB          |
| 淘汰策略 | LRU-2（默认） |
| TTL      | 1 小时        |
