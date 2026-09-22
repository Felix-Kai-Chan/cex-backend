# 🏛️ CEX Backend

一个基于 **Go + Kafka + Redis + MySQL** 实现的中小规模中心化交易所后端系统。

**核心特性：**

- ⚡ **内存撮合引擎** — 跳表 + 链表，最优价 O(1)，撤单 O(1)
- 📊 **4 种订单类型** — LIMIT / MARKET / IOC / FOK
- 🛡 **风控体系** — 熔断 + 市价单数量上限 + 滑点保护
- 🔄 **事件驱动** — Kafka + Outbox 模式，事务一致 + 按 symbol 分区
- 💾 **三层持久化** — Redis 快照 + WAL + MySQL 对账
- 📈 **可观测性** — 延迟分位（P50/P95/P99）+ TPS + 熔断状态
- 🔐 **并发安全** — 条件更新防超卖
- 🐳 **Docker 一键部署** — 4 容器编排
- ✅ **完整测试体系** — E2E + 并发 + 压测 + 崩溃恢复 + 分区验证

> 技术栈：**Go · Kafka · MySQL · Redis · WebSocket · Docker**

---

## 📋 目录

- [技术栈](#-技术栈)
- [系统架构](#-系统架构)
- [核心功能](#-核心功能)
- [项目结构](#-项目结构)
- [快速启动](#-快速启动)
- [API 接口](#-api-接口)
- [测试](#-测试)
- [踩坑记录](#-踩坑记录)
- [后续规划](#-后续规划)

---

## 🛠 技术栈

| 模块 | 技术 |
| :--- | :--- |
| 编程语言 | Go 1.25 |
| Web 框架 | Gin |
| ORM | GORM |
| 数据库 | MySQL 8.0 |
| 缓存 / 状态恢复 | Redis 7.x |
| 消息队列 | Kafka 3.7（KRaft，无 ZooKeeper） |
| Kafka 客户端 | segmentio/kafka-go |
| WebSocket | gorilla/websocket |
| 日志 | log/slog（JSON 结构化） |
| 容器化 | Docker + Docker Compose |
| 依赖管理 | Go Modules |

---

## 📐 系统架构

```text
User / Client
│
▼
┌─────────────────────────────────────────────────────────────┐
│ API 层（Gin）                                               │
│ ├── Handler：解析参数、调用 Service                         │
│ ├── WebSocket Hub：实时推送 + 断线重连补发                  │
│ └── /metrics：延迟分位 + TPS + 熔断状态                     │
└─────────────────────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│ Service 层（order.go）                                      │
│ ├── 熔断检查（IsOpen）                                       │
│ ├── 市价单保护（数量 + 滑点）                                │
│ ├── 冻结（FreezeBalance，条件更新）                          │
│ ├── 撮合（Engine.Match）                                    │
│ ├── 双边扣减 + 双边流水                                      │
│ └── 同事务写 orders + outbox                                 │
└─────────────────────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│ Engine 层（orderbook / match / skiplist / snapshot）         │
│ ├── OrderBook：每个交易对独立                                │
│ ├── 跳表：最优价 O(1)，插入/删除 O(log n)                    │
│ ├── 链表：同价格 FIFO                                        │
│ ├── orders 索引：撤单 O(1)                                   │
│ ├── 4 种订单类型（LIMIT / MARKET / IOC / FOK）                │
│ ├── FOK 预检查 + 滑点预估                                     │
│ └── 熔断器（价格波动 5% 触发，30s 冷却）                       │
└─────────────────────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│ Persistence 层                                               │
│ ├── MySQL：orders / trades / balances / ledgers / outbox     │
│ ├── Redis 快照：每 5 秒保存 OrderBook                        │
│ └── WAL 操作日志：每次订单变更追加                            │
└─────────────────────────────────────────────────────────────┘
│
▼
┌─────────────────────────────────────────────────────────────┐
│ MQ 层（Outbox 模式）                                         │
│ ├── outbox-publisher：轮询 outbox 表 → 发 Kafka              │
│ ├── Kafka topic：cex.order.events（3 partitions）            │
│ │   └── Key = symbol → 同一交易对进同一 partition             │
│ └── event-consumer：消费 Kafka，串行处理                      │
└─────────────────────────────────────────────────────────────┘
```
✨ 核心功能
1. 内存撮合引擎
数据结构：map[int64]*OrderList + 跳表（SkipList）

最优价：跳表头节点，O(1)

撤单：orders map[string]*Order 索引，O(1)

时间优先：同价格链表 FIFO

4 种订单类型：

LIMIT：限价单，挂单等成交

MARKET：市价单，立即成交，剩余丢弃

IOC：立即成交，剩余取消

FOK：全部成交，否则整单取消

2. 风控体系
熔断：单交易对价格波动超 5% → 熔断 30 秒，拒绝新单

市价单数量上限：单个市价单不超过 100（防大单冲击）

滑点保护：市价单预估成交均价 vs Best Ask/Bid 差异 > 5% → 拒绝

3. 并发安全余额管理
sql
UPDATE balances
SET available = available - ?
WHERE user_id = ? AND available >= ?
原子条件更新：数据库级别保证不超卖

RowsAffected 判断：0 表示余额不足

冻结 / 解冻 / 扣减：全部条件更新

4. 三层持久化恢复
层	机制	用途
1	Redis 快照	每 5 秒保存 OrderBook
2	WAL 操作日志	订单变更追加
3	MySQL 对账	重启后补入/移除差异
启动恢复流程：加载快照 → 回放 WAL → MySQL 对账

快照策略：锁内深拷贝，锁外序列化 + 写 Redis；存成功后清空 WAL

5. Kafka + Outbox 模式
事务一致性：写 orders + 写 outbox 在同一 MySQL 事务

发件进程：outbox-publisher 轮询 outbox 表，发 Kafka，成功标记 SENT

按 symbol 分区：Kafka key = symbol → 同一交易对的消息进同一 partition → 有序消费

幂等：outbox 表按 (id) 主键，publisher 用 status=PENDING 拉取

6. 可观测性
延迟分位：P50 / P95 / P99（滑窗 1000 样本）

TPS：最近 10 秒窗口的撮合次数 / 10

熔断状态：/metrics 输出每个交易对的熔断状态

7. WebSocket 实时推送
成交通知 / 订单状态变更

心跳检测（60 秒读超时）

断线重连 + 历史消息补发

8. 结构化日志
```json
{"time":"...","level":"INFO","msg":"match completed","order_id":"...","trades":1,"match_us":5}
{"time":"...","level":"INFO","msg":"request","method":"POST","path":"/api/v1/orders","status":200}
```

📁 项目结构
```text
cex-backend/
├── Dockerfile
├── docker-compose.yml              # 4 容器编排
├── cmd/
│   ├── api/main.go                 # API 服务入口
│   ├── outbox-publisher/main.go    # Outbox 发件进程
│   └── event-consumer/main.go      # Kafka 消费者
├── internal/
│   ├── api/
│   │   ├── handler/                # HTTP Handler
│   │   └── service/order.go        # 业务逻辑（含熔断/市价保护/outbox）
│   ├── engine/
│   │   ├── orderbook.go            # 订单簿（跳表 + 链表 + O(1) 撤单）
│   │   ├── match.go                # 撮合逻辑
│   │   ├── skiplist.go             # 跳表
│   │   ├── snapshot.go             # 快照 + WAL + 对账
│   │   ├── circuit_breaker.go      # 熔断 + 市价单保护
│   │   ├── metrics.go              # 延迟分位 + TPS
│   │   └── engine.go               # Engine 管理器
│   ├── mq/
│   │   ├── message.go              # OrderEvent 结构
│   │   └── kafka_producer.go       # Kafka Producer
│   ├── persistence/
│   │   ├── db.go                   # 连接池 + AutoMigrate
│   │   ├── balance_repo.go         # 余额（条件更新）
│   │   ├── trade_repo.go           # 订单 + 成交
│   │   ├── ledger_repo.go          # 流水
│   │   └── outbox_repo.go          # Outbox 表
│   └── websocket/
│       ├── hub.go
│       ├── client.go
│       └── message.go
├── test/
│   ├── e2e_test.sh                 # E2E
│   ├── run_all_tests.sh            # 一键全部
│   ├── stress_balance_test.go      # 并发扣减
│   ├── stress_match_test.go        # 撮合压测
│   ├── stress_recovery.sh          # 崩溃恢复
│   ├── test_wal_recovery.sh        # WAL 恢复
│   ├── test_cancel.sh              # 撤单 O(1)
│   ├── test_order_types.sh         # FOK / IOC / LIMIT
│   ├── test_metrics_tps.sh         # 延迟分位 + TPS
│   ├── test_market_protection.sh   # 市价单保护
│   ├── test_outbox.sh              # Kafka + Outbox
│   ├── test_kafka_consumer.sh      # 按 symbol 分区
│   └── reset_all.sh                # 清环境
├── screenshots/
├── go.mod
└── go.sum
```

🚀 快速启动
Docker Compose（推荐）
```bash
# 1. 启动全部服务
docker-compose up -d

# 2. 查看容器状态
docker-compose ps
# 期望：cex-mysql / cex-redis / cex-kafka / cex-api 都 Up

# 3. 健康检查
curl http://localhost:8080/health
# {"status":"ok"}

# 4. 查看监控指标
curl http://localhost:8080/metrics
服务端口：

服务	宿主机端口	容器端口
CEX API	8080	8080
MySQL	13306	3306
Redis	16379	6379
Kafka	19092	9094
本地运行
bash
# 1. 启动 MySQL + Redis + Kafka（容器）
docker-compose up -d mysql redis kafka

# 2. 启动 API
go run cmd/api/main.go

# 3. 另开终端，启动 outbox-publisher
go run cmd/outbox-publisher/main.go

# 4. 另开终端，启动 event-consumer（可选，验证用）
go run cmd/event-consumer/main.go
```

本地跑时环境变量：

```bash
export KAFKA_BROKERS=localhost:19092
export KAFKA_TOPIC=cex.order.events
📡 API 接口
方法	路径	用途
GET	/health	健康检查
GET	/metrics	延迟分位 + TPS + 熔断状态
POST	/api/v1/orders	创建订单
GET	/api/v1/orders/:id	查询订单
GET	/api/v1/orders/user/:user_id	查询用户订单
DELETE	/api/v1/orders/:id	撤单
GET	/api/v1/balance/:user_id/:asset	查询余额
POST	/api/v1/balance/add	充值
POST	/api/v1/balance/deduct	扣款
GET	/api/v1/depth	查询深度
GET	/api/v1/ledger/:user_id	查询流水
GET	/ws	WebSocket
```

示例：下单
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_A",
    "symbol": "BTC/USDT",
    "side": "BUY",
    "order_type": "LIMIT",
    "price": 50000,
    "amount": 1
  }'
示例：查 metrics

bash
curl http://localhost:8080/metrics
json
{
  "match_latency": {
    "count": 100,
    "p50_ms": 0.003,
    "p95_ms": 0.012,
    "p99_ms": 0.018,
    "tps": 2,
    "tps_window_s": 10,
    "tps_recent": 20
  },
  "circuit_breakers": {
    "BTC/USDT": {"is_open": false, "remain_sec": 0, "last_price": 0}
  }
}
```

🧪 测试
一键跑全部
```bash
./test/reset_all.sh       # 清环境
./test/run_all_tests.sh   # 跑全部
```

1. E2E 端到端
```bash
./test/e2e_test.sh
覆盖：充值 → 挂单 → 深度 → 撮合 → 双边扣减 → 撤单 → 解冻
```

2. 并发扣减
```bash
go test -v -count=1 ./test -run TestConcurrentDeduct
500 goroutine 同时扣 10 USDT → 10 成功、490 失败、余额精确归零。
```

3. 撮合压测
```bash
go test -v -count=1 ./test -run TestMatchEnginePerformance
1000 并发买单 → TPS 29.5 万，平均延迟 1.2ms。
```

4. 撮合基准
```bash
go test -bench=BenchmarkMatch -benchtime=10s ./test
单次撮合 368 ns/op。
```

5. 崩溃恢复
```bash
./test/test_wal_recovery.sh
下单 → kill -9 → 重启 → 验证 WAL replay。
```

6. 撤单 O(1)
```bash
./test/test_cancel.sh
下单 → 撤单 → 重启 → 再撤单。
```

7. 订单类型
```bash
./test/test_order_types.sh
验证 LIMIT / MARKET / IOC / FOK。
```

8. 熔断 + TPS
```bash
./test/test_metrics_tps.sh
下 20 单，验证 /metrics 输出 P50/P95/P99 + TPS。
```

9. 市价单保护
```bash
./test/test_market_protection.sh
验证数量上限 + 滑点保护。
```

10. Kafka + Outbox
```bash
./test/test_outbox.sh
下单 → outbox 表 → publisher 发 Kafka → 验证。
```
11. Kafka 按 symbol 分区
```bash
./test/test_kafka_consumer.sh
BTC × 5 + ETH × 5 → 验证 BTC 全在 partition N，ETH 全在 partition M。
```

🐛 踩坑记录
```test
坑 1：双边余额 bug（最严重）
现象：E2E 测试发现 test_A 买入后，test_B 没收到 USDT。150000 USDT 凭空消失。

根因：成交循环只处理了 order.UserID，对手方没处理。

修复：Trade 结构体新增 BuyUserID / SellUserID，成交时双边扣减 + 双边流水 + 双边订单状态更新。

坑 2：对手方订单状态不更新
现象：卖方订单部分成交后，状态一直 PENDING。

根因：成交时只更新当前订单状态，没更新对手方。

修复：从 DB 读取对手订单，根据实际 Remaining 更新状态。

坑 3：created_at 类型转换失败
现象：重启服务时对账失败，converting driver.Value type time.Time to a int64。

根因：SQL 用 int64 接收 MySQL 的 DATETIME。

修复：改成 time.Time，取 .UnixMilli()。

坑 4：最优价 O(n)
现象：早期版本遍历 map 找最优价，O(n)。

修复：引入跳表，最优价 O(1)，插入/删除 O(log n)。

坑 5：撤单 O(n)
现象：撤单遍历所有价格档找订单，O(n)。

修复：加 orders map[string]*Order 索引，O(1) 定位后摘链表。

坑 6：Redis 快照残留干扰测试
现象：清空 Redis 后跑 E2E 仍然失败。

根因：API 容器的内存 OrderBook 没清空，5 秒后又被写回 Redis。

修复：清空数据后 docker-compose restart cex-api。

坑 7：本地 MySQL/Redis/Kafka 端口冲突
现象：服务连不上 Docker 里的 MySQL。

根因：Mac 上装了本地 MySQL（3306），服务默认连 3306，但 Docker 映射到 13306。

修复：main.go 默认 DSN 改成 127.0.0.1:13306，docker-compose.yml 用 mysql:3306（容器内），通过环境变量覆盖。

坑 8：WAL 被快照误清
现象：下单后立刻查 WAL 有，等 5 秒查就没了。

根因：Save() 每次无条件 TruncateWAL()。

修复：无（设计如此——快照存成功后 WAL 已被覆盖，可清）。测试改为「下单后立刻 kill 进程」验证。

坑 9：outbox kafka_key 未填导致分区失效
现象：所有 Kafka 消息进 partition 0，按 symbol 分区失效。

根因：service/order.go 构造 OutboxModel 时漏写 KafkaKey 字段。

修复：加 KafkaKey: req.Symbol。

坑 10：Kafka advertised listener 配置
现象：容器内 docker exec 能连 Kafka，本地 go run 连不上（lookup kafka: no such host）。

根因：KAFKA_ADVERTISED_LISTENERS 只 advertised 了 kafka:9092（容器内名），本地解析不到。

修复：双 listener：

PLAINTEXT://:9092 → advertised kafka:9092（容器内）

EXTERNAL://:9094 → advertised localhost:19092（宿主机）

坑 11：Docker 构建网络超时
现象：apk add 和 go mod download 超时。

修复：Docker 构建时指定代理：
```

```bash
docker-compose build \
  --build-arg HTTP_PROXY=socks5://host.docker.internal:7897 \
  --build-arg HTTPS_PROXY=socks5://host.docker.internal:7897
```
📋 后续规划
```test
□ Prometheus + Grafana 监控接入
□ GitHub Actions CI/CD
□ 撮合引擎单写者架构（每 symbol 一个 goroutine）
□ OrderBook 增量深度推送
□ 行情服务独立化
□ Kubernetes 生产级部署
□ 多交易对分片
```

📄 License

MIT License