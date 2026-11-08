#!/bin/bash

# 退出时遇到错误立即停止
set -e

# ==========================================
# 0. 核心环境清理：强制清除终端里的代理环境变量
# 防止 Go 连 MySQL 时被本地代理劫持导致 unexpected EOF
# ==========================================
unset http_proxy
unset https_proxy
unset all_proxy
unset HTTP_PROXY
unset HTTPS_PROXY
unset ALL_PROXY
export NO_PROXY="localhost,127.0.0.1,0.0.0.0"
export no_proxy="localhost,127.0.0.1,0.0.0.0"

# 设置终端输出颜色，方便辨认
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# ==========================================
# 选项：是否在启动时清空历史数据（1=清空，0=保留）
# 如果你需要测试 WAL 重启恢复，请改成 0
CLEAN_DATA=1
# ==========================================

echo -e "${GREEN}=== 0. 检查并启动 Docker Desktop ===${NC}"
if ! docker info >/dev/null 2>&1; then
    echo -e "${YELLOW}Docker 未运行，正在自动启动 Docker Desktop...${NC}"
    open -a Docker
    echo -n "等待 Docker 引擎启动"
    for i in {1..60}; do
        if docker info >/dev/null 2>&1; then
            echo -e "\n${GREEN}Docker 启动成功！${NC}"
            break
        fi
        echo -n "."
        sleep 1
    done
    if ! docker info >/dev/null 2>&1; then
        echo -e "\n${RED}错误: Docker 启动超时，请手动打开 Docker Desktop 后重试。${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}Docker 已在运行。${NC}"
fi

# ==========================================
# 核心功能：清除历史记录
# ==========================================
if [ "$CLEAN_DATA" -eq 1 ]; then
    echo -e "${YELLOW}=== 1. 正在清除历史记录 (重置数据库和内存快照) ===${NC}"
    # 1.1 彻底删掉 MySQL/Redis 的数据卷
    docker-compose down -v >/dev/null 2>&1 || true
    
    # 1.2 删掉本地 WAL 和 Snapshot 文件
    if [ -d "data" ]; then
        rm -rf ./data/*
        echo -e "本地 WAL/Snapshot 数据已清除。"
    elif [ -d "storage" ]; then
        rm -rf ./storage/*
        echo -e "本地 WAL/Snapshot 数据已清除。"
    elif [ -d "wal" ]; then
        rm -rf ./wal/*
        echo -e "本地 WAL/Snapshot 数据已清除。"
    fi
else
    echo -e "${YELLOW}=== 1. 跳过清除历史记录 (保留现有数据) ===${NC}"
fi

echo -e "${GREEN}=== 2. 启动基础设施 (MySQL, Redis, Kafka) ===${NC}"
docker-compose up -d mysql redis kafka

echo -e "${YELLOW}正在等待 MySQL 启动就绪...${NC}"
MAX_RETRIES=60
RETRY_COUNT=0
while [ "$(docker inspect -f '{{.State.Health.Status}}' cex-mysql 2>/dev/null)" != "healthy" ]; do
    echo -n "."
    sleep 1
    RETRY_COUNT=$((RETRY_COUNT+1))
    if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
        echo -e "\n${RED}错误: MySQL 启动超时，请检查 docker logs cex-mysql${NC}"
        exit 1
    fi
done
sleep 2
echo -e "\n${GREEN}MySQL 健康检查通过，彻底就绪！${NC}"

echo -e "${GREEN}=== 3. 清理旧的 API 进程 ===${NC}"
docker stop cex-api 2>/dev/null || true
lsof -ti:8080 | xargs kill -9 2>/dev/null || true
echo -e "清理完成。"

echo -e "${GREEN}=== 4. 启动 Go API 后端 ===${NC}"
echo -e "监听端口: 8080"
echo -e "请在浏览器打开: ${YELLOW}http://localhost:8080/${NC}"
echo -e "监控指标查看: ${YELLOW}http://localhost:8080/metrics${NC}"
echo -e "按 Ctrl+C 可以停止服务。"

# 执行带环境变量的 go run
# 强制覆盖代理变量，并把本地 IP 加入 no_proxy 白名单
http_proxy="" https_proxy="" all_proxy="" \
HTTP_PROXY="" HTTPS_PROXY="" ALL_PROXY="" \
no_proxy="localhost,127.0.0.1,0.0.0.0" NO_PROXY="localhost,127.0.0.1,0.0.0.0" \
MYSQL_DSN='root:root@tcp(127.0.0.1:13306)/cex?charset=utf8mb4&parseTime=True&loc=Local' \
REDIS_ADDR='127.0.0.1:16379' \
HTTP_PORT=8080 \
go run cmd/api/main.go