#!/usr/bin/env bash
# 一键启动 CEX Gateway（含 MySQL / Redis / Kafka / API）
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

echo "=========================================="
echo "   CEX Gateway 一键启动"
echo "=========================================="

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

wait_health() {
    local url="$1"
    local name="$2"
    for i in $(seq 1 90); do
        if curl -s "$url" >/dev/null 2>&1; then
            echo "✅ ${name} 就绪"
            return 0
        fi
        sleep 2
    done
    echo "❌ ${name} 启动超时"
    return 1
}

if [ "${1:-}" = "down" ]; then
    echo "🛑 停止 Gateway / API..."
    docker-compose stop gateway cex-api
    exit 0
fi

stop_local_api() {
    echo "🧹 清理可能占用 8080 的本地 API 进程..."
    pkill -9 -f "cmd/api/main.go" 2>/dev/null || true
    pkill -9 -f "go-build.*/exe/main" 2>/dev/null || true
    pkill -9 -f "/exe/main" 2>/dev/null || true
    if command -v lsof >/dev/null 2>&1; then
        lsof -ti tcp:8080 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    fi
    sleep 1
}

ensure_docker
stop_local_api

echo "🚀 构建并启动基础设施 / API / Gateway..."
docker-compose up -d --build mysql redis kafka cex-api gateway

echo "⏳ 等待服务就绪..."
wait_health "http://localhost:8081/health" "Gateway"
wait_health "http://localhost:8080/health" "API"

echo ""
echo "=========================================="
echo "   ✅ Gateway 已启动"
echo "=========================================="
echo "Gateway:  http://localhost:8081"
echo "API:      http://localhost:8080 (仅本机)"
echo "Health:   http://localhost:8081/health"
echo "Metrics:  http://localhost:8081/metrics"
echo ""
echo "限流:     50 req/s，桶容量 100"
echo "停止:     ./run_gateway.sh down"
echo "=========================================="