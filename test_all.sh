#!/usr/bin/env bash
# 一键启动所有 cmd 服务并运行全量测试
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

KAFKA_TOPIC="${KAFKA_TOPIC:-cex.order.events}"
API_PID=""
OUTBOX_PID=""
CONSUMER_PID=""

echo "=========================================="
echo "   CEX 一键测试（含所有 cmd 服务）"
echo "=========================================="

# ---------- 基础检查 ----------
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker 未运行，请先启动 Docker Desktop"
    exit 1
fi

# ---------- 进程清理 ----------
stop_cmd_services() {
    echo "🧹 清理旧 Go 进程..."
    pkill -9 -f "cmd/api/main.go" 2>/dev/null || true
    pkill -9 -f "cmd/outbox-publisher/main.go" 2>/dev/null || true
    pkill -9 -f "cmd/event-consumer/main.go" 2>/dev/null || true
    pkill -9 -f "go-build.*/exe/main" 2>/dev/null || true
    pkill -9 -f "/exe/main" 2>/dev/null || true
    # 停掉 compose 里可能占用 8080 的旧 API 容器，避免请求打到旧镜像
    docker stop cex-api >/dev/null 2>&1 || true
    # 强制释放 8080，防止旧进程/旧二进制继续占端口
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti tcp:8080 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    fi
    if [ -n "${API_PID:-}" ]; then kill "$API_PID" 2>/dev/null || true; fi
    if [ -n "${OUTBOX_PID:-}" ]; then kill "$OUTBOX_PID" 2>/dev/null || true; fi
    if [ -n "${CONSUMER_PID:-}" ]; then kill "$CONSUMER_PID" 2>/dev/null || true; fi
    sleep 2
}

cleanup() {
    echo "🧹 清理进程..."
    stop_cmd_services
}
trap cleanup EXIT

# ---------- 基础设施 ----------
start_infra() {
    echo "🚀 启动 MySQL / Redis / Kafka..."
    docker-compose up -d mysql redis kafka

    echo "⏳ 等待基础设施就绪..."
    for i in $(seq 1 90); do
        mysql_ok=0
        redis_ok=0
        kafka_ok=0

        docker exec cex-mysql mysqladmin ping -h localhost -uroot -proot --silent >/dev/null 2>&1 && mysql_ok=1
        docker exec cex-redis redis-cli ping >/dev/null 2>&1 && redis_ok=1
        docker exec cex-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && kafka_ok=1

        if [ "$mysql_ok" = "1" ] && [ "$redis_ok" = "1" ] && [ "$kafka_ok" = "1" ]; then
            echo "✅ 基础设施就绪"
            return 0
        fi
        sleep 2
    done

    echo "❌ 基础设施启动超时"
    docker-compose ps
    exit 1
}

# ---------- 数据清理 ----------
reset_data() {
    echo "🧹 重置 MySQL / Redis / Kafka..."
    stop_cmd_services

    docker exec cex-mysql mysql -uroot -proot -e \
        "USE cex;
         TRUNCATE TABLE orders;
         TRUNCATE TABLE trades;
         TRUNCATE TABLE balances;
         TRUNCATE TABLE ledgers;
         TRUNCATE TABLE outbox;
         TRUNCATE TABLE consumed_events;" 2>/dev/null || true

    docker exec cex-redis redis-cli FLUSHALL >/dev/null 2>&1 || true

    # 清空 Kafka topic（删除后重建，避免旧消息影响断言）
    docker exec cex-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --delete --topic "$KAFKA_TOPIC" >/dev/null 2>&1 || true
    sleep 2
    docker exec cex-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 --create --topic "$KAFKA_TOPIC" \
        --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
    sleep 1

    echo "✅ 数据清理完成"
}

# ---------- 启动 cmd 服务 ----------
start_cmd_services() {
    export MYSQL_DSN="root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
    export REDIS_ADDR="127.0.0.1:16379"
    export KAFKA_BROKERS="localhost:19092"
    export KAFKA_TOPIC
    export HTTP_PORT="8080"

    echo "🚀 启动 cmd/api..."
    go run cmd/api/main.go > /tmp/cex_api.log 2>&1 &
    API_PID=$!

    echo "🚀 启动 cmd/outbox-publisher..."
    go run cmd/outbox-publisher/main.go > /tmp/cex_outbox.log 2>&1 &
    OUTBOX_PID=$!

    echo "🚀 启动 cmd/event-consumer..."
    go run cmd/event-consumer/main.go > /tmp/cex_consumer.log 2>&1 &
    CONSUMER_PID=$!

    echo "⏳ 等待 API 就绪..."
    for i in $(seq 1 30); do
        if curl -s http://localhost:8080/health >/dev/null 2>&1; then
            echo "✅ API 就绪"
            return 0
        fi
        sleep 1
    done

    echo "❌ API 启动失败，日志如下："
    tail -30 /tmp/cex_api.log || true
    exit 1
}

restart_all_services() {
    stop_cmd_services
    reset_data
    start_cmd_services
}

# ---------- 主流程 ----------
start_infra
reset_data

echo "🔨 编译项目..."
go build ./...

start_cmd_services

echo ""
echo "[1/5] Go 单元测试..."
go test -count=1 ./test

echo ""
echo "[2/5] E2E 集成测试..."
bash test/e2e_test.sh

echo ""
echo "[3/5] IOC/FOK 测试..."
bash test/test_ioc_fok.sh

echo ""
restart_all_services

echo ""
echo "[4/5] 订单类型测试..."
bash test/test_order_types.sh

echo ""
stop_cmd_services
reset_data

echo ""
echo "[5/5] Outbox 测试..."
bash test/test_outbox.sh

echo ""
echo "=========================================="
echo "   ✅ 全部测试完成（5/5）"
echo "=========================================="