#!/usr/bin/env bash
# Gateway 限流与反向代理基础测试
set -euo pipefail

GATEWAY="${GATEWAY_URL:-http://localhost:8081}"
API="${API_URL:-http://localhost:8080}"

echo "=========================================="
echo "   Gateway 限流测试"
echo "=========================================="

echo "[1/4] 健康检查..."
HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/health" || true)
if [ "$HEALTH" != "200" ]; then
    echo "❌ Gateway health 失败: HTTP $HEALTH"
    exit 1
fi
echo "✅ Gateway health 正常"

echo "[2/4] 反向代理检查..."
PROXY_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/api/v1/depth?symbol=BTC%2FUSDT&limit=5" || true)
if [ "$PROXY_CODE" != "200" ]; then
    echo "❌ 代理到 API 失败: HTTP $PROXY_CODE"
    exit 1
fi
echo "✅ 反向代理正常"

echo "[3/4] 并发限流检查（300 次快速请求）..."
TMP=$(mktemp)
seq 1 300 | xargs -P 50 -I{} curl -s -o /dev/null -w "%{http_code}\n" \
    "$GATEWAY/api/v1/depth?symbol=BTC%2FUSDT&limit=5" > "$TMP" || true

OK_COUNT=$(grep -c '^200$' "$TMP" || true)
LIMIT_COUNT=$(grep -c '^429$' "$TMP" || true)
OTHER_COUNT=$(grep -vc -E '^(200|429)$' "$TMP" || true)

echo "   200: $OK_COUNT"
echo "   429: $LIMIT_COUNT"
echo "   other: $OTHER_COUNT"

if [ "$OK_COUNT" -eq 0 ]; then
    echo "❌ 没有任何请求通过"
    rm -f "$TMP"
    exit 1
fi
if [ "$LIMIT_COUNT" -eq 0 ]; then
    echo "❌ 没有触发 429，限流可能未生效"
    rm -f "$TMP"
    exit 1
fi
echo "✅ 限流生效"

echo "[4/4] 等待令牌恢复..."
sleep 2
RECOVER_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$GATEWAY/health" || true)
if [ "$RECOVER_CODE" != "200" ]; then
    echo "❌ health 恢复检查失败"
    rm -f "$TMP"
    exit 1
fi
echo "✅ Gateway 正常"

rm -f "$TMP"
echo ""
echo "=========================================="
echo "   ✅ Gateway 测试通过"
echo "=========================================="