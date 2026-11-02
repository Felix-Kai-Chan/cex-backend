#!/usr/bin/env bash
# 一键压测：启动基础设施 + API，运行 test/benchmark_concurrent.go，结束后清理 API
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

API_PID=""

echo "=========================================="
echo "   CEX 一键并发压测"
echo "=========================================="

# ---------- 基础检查 ----------
ensure_docker() {
    if docker info >/dev/null 2>&1; then
        return 0
    fi

    echo "🐳 Docker 未运行，尝试启动 Docker Desktop..."
    if [[ "$OSTYPE" == darwin* ]]; then
        open -a Docker >/dev/null 2>&1 || true
    fi

    for i in $(seq 1 60); do
        if docker info >/dev/null 2>&1; then
            echo "✅ Docker 已启动"
            return 0
        fi
        sleep 2
    done

    echo "❌ Docker 启动失败，请手动打开 Docker Desktop"
    exit 1
}

# ---------- 进程清理 ----------
stop_api() {
    echo "🧹 清理 API 进程..."
    pkill -9 -f "cmd/api/main.go" 2>/dev/null || true
    pkill -9 -f "go-build.*/exe/main" 2>/dev/null || true
    pkill -9 -f "/exe/main" 2>/dev/null || true
    # 停掉 compose 里可能占用 8080 的旧 API 容器
    docker stop cex-api >/dev/null 2>&1 || true
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti tcp:8080 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    fi
    if [ -n "${API_PID:-}" ]; then
        kill "$API_PID" 2>/dev/null || true
    fi
    sleep 2
}

cleanup() {
    stop_api
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
    echo "🧹 清理 MySQL / Redis..."
    docker exec cex-mysql mysql -uroot -proot -e \
        "USE cex;
         TRUNCATE TABLE orders;
         TRUNCATE TABLE trades;
         TRUNCATE TABLE balances;
         TRUNCATE TABLE ledgers;
         TRUNCATE TABLE outbox;
         TRUNCATE TABLE consumed_events;" 2>/dev/null || true

    docker exec cex-redis redis-cli FLUSHALL >/dev/null 2>&1 || true
    echo "✅ 数据清理完成"
}

# ---------- 启动 API ----------
start_api() {
    export MYSQL_DSN="root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
    export REDIS_ADDR="127.0.0.1:16379"
    export KAFKA_BROKERS="localhost:19092"
    export KAFKA_TOPIC="cex.order.events"
    export HTTP_PORT="8080"
export CEX_ACCESS_LOG="0"

    echo "🚀 启动 cmd/api..."
    go run cmd/api/main.go > /tmp/cex_benchmark_api.log 2>&1 &
    API_PID=$!

    echo "⏳ 等待 API 就绪..."
    for i in $(seq 1 30); do
        if curl -s http://localhost:8080/health >/dev/null 2>&1; then
            echo "✅ API 就绪"
            return 0
        fi
        if ! kill -0 "$API_PID" 2>/dev/null; then
            echo "❌ API 进程已退出，日志如下："
            tail -30 /tmp/cex_benchmark_api.log || true
            exit 1
        fi
        sleep 1
    done

    echo "❌ API 启动超时，日志如下："
    tail -30 /tmp/cex_benchmark_api.log || true
    exit 1
}

# ---------- 主流程 ----------
ensure_docker
start_infra
stop_api
reset_data

echo "🔨 编译项目..."
go build ./...

start_api

echo ""
echo "🔥 开始压测..."
echo "   并发:   ${BENCH_CONCURRENCY:-300}"
echo "   时长:   ${BENCH_DURATION:-10s}"
echo "   买家:   ${BENCH_BUYERS:-100}"
echo "   卖家:   ${BENCH_SELLERS:-100}"
echo ""

BENCH_URL="${BENCH_URL:-http://localhost:8080/api/v1}" \
BENCH_CONCURRENCY="${BENCH_CONCURRENCY:-300}" \
BENCH_DURATION="${BENCH_DURATION:-10s}" \
BENCH_BUYERS="${BENCH_BUYERS:-100}" \
BENCH_SELLERS="${BENCH_SELLERS:-100}" \
go run test/benchmark_concurrent.go

echo ""
echo "=========================================="
echo "   ✅ 压测完成"
echo "=========================================="