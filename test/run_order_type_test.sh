#!/bin/bash

# 一键：清环境 + 启动服务 + 跑测试
# 用法：./test/run_order_type_test.sh

set -e

cd "$(dirname "$0")/.."

echo "=========================================="
echo " 一键测试：清环境 + 启动服务 + 跑测试"
echo "=========================================="
echo ""

# 1. 停服务
echo "[1/5] 停服务..."
pkill -9 -f "go-build.*main" 2>/dev/null || true
pkill -9 -f "go run cmd/api/main.go" 2>/dev/null || true
sleep 1

if pgrep -f "go-build.*main" > /dev/null; then
    echo "❌ 杀不干净"
    ps aux | grep "go-build.*main" | grep -v grep
    exit 1
fi
echo "✅ 服务已停"
echo ""

# 2. 清 MySQL
echo "[2/5] 清 MySQL..."
docker exec cex-mysql mysql -uroot -proot cex \
    -e "TRUNCATE TABLE orders; TRUNCATE TABLE trades;" 2>/dev/null
echo "✅ MySQL 已清"
echo ""

# 3. 清 Redis
echo "[3/5] 清 Redis..."
redis-cli -p 6379 FLUSHDB > /dev/null
echo "✅ Redis 已清"
echo ""

# 4. 启动服务
echo "[4/5] 启动服务..."
go run cmd/api/main.go > /tmp/cex_test_server.log 2>&1 &
SERVER_PID=$!

# 等 API 就绪（最多 15 秒）
for i in $(seq 1 15); do
    if curl -s http://localhost:8080/health > /dev/null 2>&1; then
        echo "✅ 服务已就绪（等了 ${i}s）"
        break
    fi
    sleep 1
    if [ $i -eq 15 ]; then
        echo "❌ 服务启动超时，日志："
        cat /tmp/cex_test_server.log
        exit 1
    fi
done
echo ""

# 显示启动日志关键行
echo "启动日志："
grep -E "rebuilt|reconcile|snapshot restored|WAL" /tmp/cex_test_server.log || true
echo ""

# 5. 跑测试
echo "[5/5] 跑订单类型测试..."
echo ""
./test/test_order_types.sh
TEST_RESULT=$?

# 清理
echo ""
echo "=========================================="
if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ 全部通过"
else
    echo "❌ 测试失败"
fi
echo "=========================================="
echo ""
echo "Server log: /tmp/cex_test_server.log"
echo "Server PID: $SERVER_PID"
echo "To stop: kill $SERVER_PID"
echo ""

exit $TEST_RESULT