#!/bin/bash

set -euo pipefail

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

# 1. Go 引擎与恢复测试（包含 snapshot_restore_test.go / snapshot_mysql_test.go）
echo -e "\n[1/7] Go 引擎与恢复测试..."
go test ./internal/engine -count=1

# 2. E2E
echo -e "\n[2/7] E2E 功能测试..."
./test/e2e_test.sh

# 3. 并发扣减
echo -e "\n[3/7] 并发扣减测试..."
go test -v -count=1 ./test -run TestConcurrentDeduct 2>&1 | grep -v "\[mysql\]"

# 4. 撮合压测
echo -e "\n[4/7] 撮合引擎压测..."
go test -v -count=1 ./test -run TestMatchEnginePerformance

# 5. 崩溃恢复
echo -e "\n[5/7] 崩溃恢复测试..."
./test/stress_recovery.sh

echo -e "\n[6/7] MySQL 按交易对重建恢复测试..."
./test/test_recovery_rebuild.sh

echo -e "\n[7/7] 恢复失败拒绝启动测试..."
./test/test_recovery_fail_fast.sh

echo -e "\n=========================================="
echo "   ✅ 全部测试通过"
echo "=========================================="