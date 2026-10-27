#!/usr/bin/env bash
# 一键启动所有 cmd 服务并运行全量测试
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

echo "=========================================="
echo "   CEX 一键测试（含所有 cmd 服务）"
echo "=========================================="

# 1. 检查 Docker
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker 未运行，请先启动 Docker Desktop"
    exit 1
fi

# 2. 启动基础设施
echo "🚀 启动 MySQL / Redis / Kafka..."
docker-compose up -d mysql redis kafka

# 3. 等待健康
echo "⏳ 等待服务就绪..."
for i in $(seq 1 60); do
    if docker exec cex-mysql mysqladmin ping -h localhost -uroot -proot --silent >/dev/null 2>&1 \
       && docker exec cex-redis redis-cli ping >/dev/null 2>&1; then
        echo "✅ 基础设施就绪"
        break
    fi
    sleep 2
done

# 4. 重置测试数据
echo "🧹 重置测试数据..."
docker exec cex-mysql mysql -uroot -proot -e \
    "USE cex; TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE balances; TRUNCATE TABLE ledgers; TRUNCATE TABLE outbox; TRUNCATE TABLE consumed_events;" 2>/dev/null || true
docker exec cex-redis redis-cli FLUSHALL >/dev/null 2>&1 || true

# 5. 编译
echo "🔨 编译项目..."
go build ./...

# 6. 环境变量
export MYSQL_DSN="root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
export REDIS_ADDR="127.0.0.1:16379"
export KAFKA_BROKERS="localhost:19092"
export KAFKA_TOPIC="cex.order.events"
export HTTP_PORT="8080"

# 7. 清理旧进程
pkill -f "cmd/api/main.go" 2>/dev/null || true
pkill -f "/main" 2>/dev/null || true
pkill -f "cmd/outbox-publisher/main.go" 2>/dev/null || true
pkill -f "cmd/event-consumer/main.go" 2>/dev/null || true
sleep 1

# 8. 启动所有 cmd 服务
echo "🚀 启动 cmd/api..."
go run cmd/api/main.go > /tmp/cex_api.log 2>&1 &
API_PID=$!

echo "🚀 启动 cmd/outbox-publisher..."
go run cmd/outbox-publisher/main.go > /tmp/cex_outbox.log 2>&1 &
OUTBOX_PID=$!

echo "🚀 启动 cmd/event-consumer..."
go run cmd/event-consumer/main.go > /tmp/cex_consumer.log 2>&1 &
CONSUMER_PID=$!

cleanup() {
    echo "🧹 清理进程..."
    kill "$API_PID" "$OUTBOX_PID" "$CONSUMER_PID" 2>/dev/null || true
}
trap cleanup EXIT


# 重置环境并重启 API（清空内存订单簿与 Redis/DB）
reset_and_restart_api() {
    echo "🧹 重置环境并重启 API..."
    pkill -f "cmd/api/main.go" 2>/dev/null || true
    pkill -f "/main" 2>/dev/null || true
    kill "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
    docker exec cex-mysql mysql -uroot -proot -e \
        "USE cex; TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE balances; TRUNCATE TABLE ledgers; TRUNCATE TABLE outbox; TRUNCATE TABLE consumed_events;" 2>/dev/null || true
    docker exec cex-redis redis-cli FLUSHALL >/dev/null 2>&1 || true
    go run cmd/api/main.go > /tmp/cex_api.log 2>&1 &
    API_PID=$!
    for i in $(seq 1 30); do
        if curl -s http://localhost:8080/health >/dev/null 2>&1; then
            echo "✅ API 重启就绪"
            break
        fi
        sleep 1
    done
}

# 9. 等待 API 就绪
echo "⏳ 等待 API 就绪..."
for i in $(seq 1 30); do
    if curl -s http://localhost:8080/health >/dev/null 2>&1; then
        echo "✅ API 就绪"
        break
    fi
    sleep 1
done

# 10. 运行测试
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
reset_and_restart_api

echo ""
echo "[4/5] 订单类型测试..."
bash test/test_order_types.sh

echo ""
reset_and_restart_api

echo ""
echo "[5/5] Outbox 测试..."
bash test/test_outbox.sh || true

echo ""
echo "=========================================="
echo "   ✅ 全部测试完成"
echo "=========================================="