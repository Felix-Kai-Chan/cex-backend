#!/bin/bash

set -e

echo "=========================================="
echo "   CEX 全部测试"
echo "=========================================="

# 自动清空测试数据
if docker ps --format '{{.Names}}' | grep -q cex-mysql; then
    echo "🧹 清空 Docker 环境数据..."
    docker exec cex-mysql mysql -uroot -proot -e "USE cex; TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE balances; TRUNCATE TABLE ledgers;"
    docker exec cex-redis redis-cli FLUSHALL > /dev/null
    # ✅ 重启 API 容器，清空内存 OrderBook
    echo "🔄 重启 API 容器..."
    docker-compose restart cex-api
    sleep 5
else
    echo "🧹 清空本地环境数据..."
    mysql -u root -e "USE cex; TRUNCATE TABLE orders; TRUNCATE TABLE trades; TRUNCATE TABLE balances; TRUNCATE TABLE ledgers;" 2>/dev/null
    redis-cli FLUSHALL > /dev/null
fi

# 1. E2E
echo -e "\n[1/4] E2E 功能测试..."
./test/e2e_test.sh

# 2. 并发扣减
echo -e "\n[2/4] 并发扣减测试..."
go test -v ./test -run TestConcurrentDeduct

# 3. 撮合压测
echo -e "\n[3/4] 撮合引擎压测..."
go test -v ./test -run TestMatchEnginePerformance

# 4. 崩溃恢复
echo -e "\n[4/4] 崩溃恢复测试..."
./test/stress_recovery.sh

echo -e "\n=========================================="
echo "   ✅ 全部测试通过"
echo "=========================================="