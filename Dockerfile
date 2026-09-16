# ============================================
# Stage 1：构建
# ============================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# 换 Alpine 国内源（阿里云）
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

# 安装依赖
RUN apk add --no-cache git

# 复制 go.mod / go.sum 先下载依赖（利用 Docker 缓存）
COPY go.mod go.sum ./
RUN go mod download

# 复制源码
COPY . .

# 编译（静态链接，无 CGO）
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/cex-api ./cmd/api

# ============================================
# Stage 2：运行
# ============================================
FROM alpine:3.19

WORKDIR /app

# 换 Alpine 国内源（阿里云）
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

# 安装 ca-certificates（HTTPS 请求需要）
RUN apk add --no-cache ca-certificates tzdata

# 从 builder 复制二进制
COPY --from=builder /app/cex-api /app/cex-api

# 设置时区
ENV TZ=Asia/Shanghai

# 暴露端口
EXPOSE 8080

# 启动
CMD ["/app/cex-api"]