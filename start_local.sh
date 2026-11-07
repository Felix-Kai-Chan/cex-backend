#!/bin/bash

# 退出时遇到错误立即停止
set -e

# 设置终端输出颜色，方便辨认
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 1. 启动基础设施 (MySQL, Redis, Kafka) ===${NC}"
docker-compose up -d mysql redis kafka

echo -e "${YELLOW}正在等待 MySQL 和 Redis 启动就绪...${NC}"
# 简单的等待机制，确保数据库起来了再跑 Go
sleep 3 

echo -e "${GREEN}=== 2. 清理旧的 API 进程 ===${NC}"
# 尝试停掉可能还在后台跑的 cex-api 容器（如果有的话）
docker stop cex-api 2>/dev/null || true
# 顺手清理一下可能残留的 go run 进程 (针对 8080 端口)
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
echo -e "清理完成。"

echo -e "${GREEN}=== 3. 启动 Go API 后端 ===${NC}"
echo -e "监听端口: 8080"
echo -e "请在浏览器打开: ${YELLOW}http://localhost:8080/${NC}"
echo -e "监控指标查看: ${YELLOW}http://localhost:8080/metrics${NC}"
echo -e "按 Ctrl+C 可以停止服务。"

# 执行带环境变量的 go run
MYSQL_DSN='root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local' \
REDIS_ADDR='127.0.0.1:16379' \
HTTP_PORT=8080 \
go run cmd/api/main.go