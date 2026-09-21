#!/bin/bash

# 完全清理脚本：停服务 + 清 MySQL + 清 Redis + 验证
# 用法：./test/reset_all.sh

set -e

MYSQL_CONTAINER="cex-mysql"
MYSQL_USER="root"
MYSQL_PASSWORD="root"
MYSQL_DB="cex"
REDIS_PORT="6379"

echo "=========================================="
echo " 完全清理脚本"
echo "=========================================="
echo ""

# 1. 停所有 cex 服务进程
echo "[1/5] 停止所有服务进程..."
pkill -f "go run cmd/api/main.go" 2>/dev/null || true
pkill -f "go-build.*main" 2>/dev/null || true
sleep 1

# 强杀残留
if pgrep -f "go-build.*main" > /dev/null; then
    echo "   有残留进程，强杀"
    pkill -9 -f "go-build.*main" 2>/dev/null || true
    sleep 1
fi

if pgrep -f "go-build.*main" > /dev/null; then
    echo "❌ 杀不干净，请手动 kill"
    ps aux | grep "go-build.*main" | grep -v grep
    exit 1
fi
echo "✅ 服务已停"
echo ""

# 2. 清 MySQL
echo "[2/5] 清空 MySQL orders + trades..."
docker exec "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    -e "TRUNCATE TABLE orders; TRUNCATE TABLE trades;" 2>/dev/null
echo "✅ MySQL 已清空"
echo ""

# 3. 清 Redis
echo "[3/5] 清空 Redis..."
redis-cli -p "$REDIS_PORT" FLUSHDB > /dev/null
echo "✅ Redis 已清空"
echo ""

# 4. 验证
echo "[4/5] 验证清理结果..."

ORDERS_COUNT=$(docker exec "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DB" \
    -e "SELECT COUNT(*) FROM orders;" 2>/dev/null | tail -1)

REDIS_KEYS=$(redis-cli -p "$REDIS_PORT" DBSIZE)

echo "   MySQL orders 行数: $ORDERS_COUNT"
echo "   Redis key 数量: $REDIS_KEYS"
echo ""

if [ "$ORDERS_COUNT" != "0" ]; then
    echo "❌ MySQL 没清干净"
    exit 1
fi

if [ "$REDIS_KEYS" != "0" ]; then
    echo "❌ Redis 没清干净"
    redis-cli -p "$REDIS_PORT" KEYS "*"
    exit 1
fi

echo "✅ 清理完成"
echo ""

# 5. 提示
echo "[5/5] 下一步"
echo "=========================================="
echo " 清理完成，现在可以："
echo "=========================================="
echo ""
echo "1. 重启服务："
echo "   go run cmd/api/main.go"
echo ""
echo "2. 等日志出现 'CEX API 启动' 后，再跑测试"
echo ""