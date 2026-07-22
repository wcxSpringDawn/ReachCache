# 单机示例：用户信息服务

演示 ReachCache 作为 Go 业务进程与数据库之间的**嵌入缓存**。

## 架构

```
HTTP :8080  →  Group.Get("users", key)
                ├── Cache Hit → 直接返回 JSON
                └── Cache Miss → Getter → SQLite 查询 → 回写缓存 → 返回
```

## 运行

```bash
cd examples/single
go run main.go
```

默认监听 `:8080`，可通过 `HTTP_ADDR` 环境变量修改：

```bash
HTTP_ADDR=:9090 go run main.go
```

## API

### GET /user/:id

```bash
# 第一次 — SQLite 回源（日志会打出 [DB] query）
Invoke-WebRequest -Uri "http://localhost:8080/user/1"

# 第二次 — 本地缓存命中（无 DB 日志）
Invoke-WebRequest -Uri "http://localhost:8080/user/1"
```

### PUT /user/:id?name=&age=

```bash
# 同时写数据库 + 更新缓存
Invoke-WebRequest -Method PUT -Uri "http://localhost:8080/user/1?name=Alice&age=25"
```

### GET /stats

```bash
curl http://localhost:8080/stats
```

## 缓存配置

| 参数     | 值            |
| -------- | ------------- |
| Group 名 | `users`       |
| 容量     | 64MB          |
| 淘汰策略 | LRU-2（默认） |
| TTL      | 10 分钟       |

## 预期输出

首次请求会打出 `[DB] query user id=1`，再次请求同一 ID 则无 DB 日志，说明缓存命中。
