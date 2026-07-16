# 🏛️ CEX Backend

一个基于 Go 实现的中小规模中心化交易所后端系统。

项目包含：

* 内存订单簿（OrderBook）
* 撮合引擎（Matching Engine）
* 限价单 / 市价单交易
* 用户余额管理与资金冻结
* WebSocket 实时交易推送
* Redis 订单簿状态快照恢复

> 该项目为个人独立开发，用于展示 Go 后端开发能力、并发控制、系统设计以及交易系统工程实践。

---

# 📋 目录

* [技术栈](#技术栈)
* [系统架构](#系统架构)
* [交易流程](#交易流程)
* [核心功能](#核心功能)
* [设计要点](#设计要点)
* [项目结构](#项目结构)
* [快速启动](#快速启动)
* [测试](#测试)
* [当前限制](#当前限制)
* [后续规划](#后续规划)

---

# 🛠 技术栈

| 模块        | 技术                |
| --------- | ----------------- |
| 编程语言      | Go 1.25           |
| Web框架     | Gin               |
| ORM       | GORM              |
| 数据库       | MySQL 8.0         |
| 缓存 / 状态恢复 | Redis 7.x         |
| Redis客户端  | go-redis/v9       |
| WebSocket | gorilla/websocket |
| 容器化       | Docker            |
| 依赖管理      | Go Modules        |

---

# 📐 系统架构

```
Client
  |
  |
  ▼
Gin Router
  |
  |
  ▼
Handler Layer
  |
  |
  ▼
OrderService
  |
  |
  ├───────────────┬────────────────┐
  ▼               ▼                ▼
Matching       WebSocket       Persistence
Engine            Hub              |
  |                                |
  ▼                                ▼
OrderBook                         MySQL


OrderBook
    |
    ▼
Snapshotter
    |
    ▼
Redis
```

---

# 🔄 交易流程

一次订单请求流程：

```
Client
  |
  ▼
REST API
  |
  ▼
Gin Handler
  |
  ▼
OrderService
  |
  ▼
Matching Engine
  |
  ▼
OrderBook
  |
  ├── 成交
  |      |
  |      ├── 保存 Trade
  |      ├── 更新 Order 状态
  |      ├── 更新 Balance
  |      └── 记录 Ledger
  |
  └── 未完全成交
         |
         ▼
     进入订单簿等待匹配
```

完整流程：

1. 用户提交订单
2. Handler 接收并校验请求
3. OrderService 获取对应交易对 OrderBook
4. Matching Engine 执行价格优先、时间优先撮合
5. 成交结果写入 MySQL
6. WebSocket 推送交易状态
7. Redis 定时保存订单簿状态

---

# ✨ 核心功能

## 1. REST API

支持：

* ✅ 创建订单
* ✅ 查询订单
* ✅ 查询用户订单
* ✅ 撤销订单
* ✅ 查询交易深度
* ✅ 查询余额
* ✅ 查询资金流水

支持订单类型：

* LIMIT 限价单
* MARKET 市价单

---

# 2. 撮合引擎

实现：

* ✅ 多交易对支持

例如：

```
BTC/USDT
ETH/USDT
```

* ✅ 买卖订单分离

```
bids   -> 买盘
asks   -> 卖盘
```

* ✅ 价格优先

买方：

```
最高买价优先成交
```

卖方：

```
最低卖价优先成交
```

* ✅ 时间优先

同价格订单：

```
FIFO
先进先出
```

* ✅ 支持部分成交

例如：

卖单：

```
10 BTC
```

买单：

```
3 BTC
```

成交后：

```
剩余卖单 7 BTC
```

继续留在订单簿。

---

# 3. 数据持久化

MySQL 保存：

## orders

订单历史：

```
订单ID
用户ID
方向
价格
数量
状态
```

状态：

```
PENDING
PARTIAL
FILLED
CANCELLED
```

## trades

成交记录：

```
买单ID
卖单ID
成交价格
成交数量
时间
```

## balances

用户资产：

```
available
frozen
total
```

## ledgers

资金流水：

```
充值
交易
解冻
成交
```

---

# 4. 并发安全设计

余额扣减采用条件更新：

```sql
UPDATE balances
SET available = available - amount
WHERE user_id = ?
AND available >= amount
```

通过：

* SQL 原子更新
* RowsAffected 判断

避免：

* 并发超卖
* 余额不足情况下重复扣减

---

# 5. WebSocket 实时推送

实现：

* 成交通知
* 订单状态变化
* 心跳检测

结构：

```
Client

  |
  ▼

WebSocket Hub

  |
  ▼

Broadcast Channel

  |
  ▼

Connected Clients
```

---

# 6. Redis 状态快照恢复

由于订单簿运行在内存：

```
Memory OrderBook
```

服务重启后会丢失状态。

因此增加：

```
OrderBook
      |
      ▼
Snapshotter
      |
      ▼
Redis
```

功能：

* 每 5 秒保存订单簿快照
* 服务启动加载快照
* 恢复未完成订单

---

# 📁 项目结构

```
cex-backend/

├── cmd/
│   └── api/
│       └── main.go

├── internal/

│   ├── api/
│   │   ├── handler/
│   │   └── service/

│   ├── engine/
│   │   ├── orderbook.go
│   │   ├── matcher.go
│   │   └── snapshot.go

│   ├── persistence/
│   │   ├── order.go
│   │   ├── balance.go
│   │   └── ledger.go

│   └── websocket/
│       ├── hub.go
│       ├── client.go
│       └── message.go


├── test/

├── screenshots/

├── go.mod
└── go.sum
```

---

# 🚀 快速启动

## 1. 启动 MySQL

```bash
docker run -d \
--name mysql-cex \
-e MYSQL_ROOT_PASSWORD=root \
-p 3306:3306 \
mysql:8.0
```

---

## 2. 启动 Redis

```bash
docker run -d \
--name redis-cex \
-p 6379:6379 \
redis:7
```

---

## 3. 创建数据库

```bash
mysql -u root -p \
-e "CREATE DATABASE cex;"
```

---

## 4. 启动服务

```bash
go mod tidy

go run cmd/api/main.go
```

---

# 🧪 测试

## 并发余额测试

测试：

10 个 goroutine 同时扣减余额。

结果：

```
初始余额:
100 USDT

并发请求:
10 × 扣减60

最终:

成功:
1

余额:
40 USDT
```

证明：

乐观锁控制生效。

运行：

```bash
go test -v ./test
```

---

## 端到端测试

流程：

```
充值

↓

创建卖单

↓

查看深度

↓

市价买入

↓

撮合成交

↓

查询余额

↓

查询流水

↓

撤单
```

运行：

```bash
chmod +x test/e2e_test.sh

./test/e2e_test.sh
```

---

# ⚠️ 当前限制

当前版本为单节点交易系统。

存在以下限制：

## 1. OrderBook 查询复杂度

目前：

```
map[int64]*OrderList
```

通过遍历获取最优价格。

复杂度：

```
O(n)
```

生产环境可以升级：

* TreeMap
* SkipList
* Heap
* Redis Sorted Set

---

## 2. 单节点撮合

当前：

```
Single Matching Engine
```

未来可以通过：

```
MQ
+
分布式撮合节点
```

扩展。

---

# 📋 后续规划

* [ ] Docker Compose 完整部署
* [ ] Prometheus + Grafana 监控
* [ ] GitHub Actions CI/CD
* [ ] RabbitMQ 异步消息系统
* [ ] Redis Pub/Sub 多节点扩展
* [ ] OrderBook 增量深度推送
* [ ] 行情服务独立化

---

# 📄 License

MIT License
