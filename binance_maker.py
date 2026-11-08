import json
import threading
import time
import os
import requests
import websocket

# ================= 配置 =================
# ⚠️ 核心修正：在代码最开头强制设置代理环境变量
# 这样无论是 requests 还是 websocket，都会自动走这个梯子
os.environ['http_proxy'] = 'http://127.0.0.1:7897'
os.environ['https_proxy'] = 'http://127.0.0.1:7897'
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7897'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7897'
# 但是排除本地 CEX 的请求，防止把本地请求也代理走了
os.environ['no_proxy'] = '127.0.0.1,localhost'
os.environ['NO_PROXY'] = '127.0.0.1,localhost'

API_URL = "http://localhost:8080/api/v1/orders" 
BOT_BUYER = "bot_buyer"
BOT_SELLER = "bot_seller"
SYMBOL = "BTC/USDT"
ORDER_AMOUNT = 1

LATEST_PRICE = 65000.00
PRICE_READY = False

# ================= CEX 下单函数 =================
def place_order(user_id, side, price, quantity):
    payload = {
        "user_id": user_id,
        "symbol": SYMBOL,
        "side": side,
        "order_type": "LIMIT",
        "time_in_force": "GTC",
        "price": int(round(price)),
        "amount": int(quantity)
    }
    try:
        r = requests.post(API_URL, json=payload, timeout=3)
        if r.status_code == 200:
            data = r.json()
            print(f"✅ [{side}] {user_id} 挂单成功 | 价格: {int(round(price))} | 状态: {data.get('status')}")
        elif r.status_code == 429:
            print(f"⚠️ [{side}] 触发限流 429！")
        else:
            print(f"❌ [{side}] 失败: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"❌ 网络错误: {e}")

# ================= 币安 WebSocket =================
def on_message(ws, message):
    global LATEST_PRICE, PRICE_READY
    try:
        data = json.loads(message)
        if 'c' in data:
            LATEST_PRICE = float(data['c'])
            PRICE_READY = True
            print(f"📈 币安实时价格: {LATEST_PRICE} USDT")
    except Exception as e:
        print(f"解析行情失败: {e}")

def on_error(ws, error):
    print(f"WebSocket 错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket 已断开")

def on_open(ws):
    print("🚀 成功连接币安 WebSocket，开始接收行情...")

def start_binance_ws():
    socket_url = "wss://stream.binance.com:9443/ws/btcusdt@miniTicker"
    while True:
        # 移除了报错的参数，让 websocket 自动读取刚才设置的环境变量
        ws = websocket.WebSocketApp(socket_url,
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close)
        ws.run_forever(ping_interval=20, ping_timeout=10)
        print("🔄 币安 WebSocket 已断开，5 秒后重连...")
        time.sleep(5)

# ================= 交易机器人主循环 =================
def trading_loop():
    global LATEST_PRICE, PRICE_READY
    while True:
        if PRICE_READY and LATEST_PRICE > 0:
            buy_price = LATEST_PRICE + 5.0
            sell_price = LATEST_PRICE - 5.0
            qty = ORDER_AMOUNT
            
            t1 = threading.Thread(target=place_order, args=(BOT_BUYER, "BUY", buy_price, qty))
            t2 = threading.Thread(target=place_order, args=(BOT_SELLER, "SELL", sell_price, qty))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            
        time.sleep(1)

if __name__ == "__main__":
    print("=== CEX 真实行情驱动模拟器启动 ===")
    print("💡 请确保 bot_buyer 和 bot_seller 账户里已有资金！")
    
    ws_thread = threading.Thread(target=start_binance_ws)
    ws_thread.daemon = True
    ws_thread.start()
    
    time.sleep(2)
    
    try:
        trading_loop()
    except KeyboardInterrupt:
        print("\n🛑 程序已手动停止。")