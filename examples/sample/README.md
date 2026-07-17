# Quick Start

## Preparation

你需要自己启动etcd服务，并在下面的ETCD_ADDR=ip:host中填入服务地址。

## go run

$env:NODE_ID="A"; $env:NODE_PORT="8001"; $env:STATS_PORT="18001"; $env:ETCD_ADDR="ip:2379"; $env:ADVERTISE_HOST="127.0.0.1"; go run ./examples/sample/

$env:NODE_ID="B"; $env:NODE_PORT="8002"; $env:STATS_PORT="18002"; $env:ETCD_ADDR="ip:2379"; $env:ADVERTISE_HOST="127.0.0.1"; go run ./examples/sample/

$env:NODE_ID="C"; $env:NODE_PORT="8003"; $env:STATS_PORT="18003"; $env:ETCD_ADDR="ip:2379"; $env:ADVERTISE_HOST="127.0.0.1"; go run ./examples/sample/
