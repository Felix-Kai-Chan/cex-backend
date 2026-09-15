package main

import (
	"fmt"
	"log"
	"net/http"

	"cex-backend/internal/api/handler"
	"cex-backend/internal/api/service"
	"cex-backend/internal/engine"
	"cex-backend/internal/persistence"
	"cex-backend/internal/websocket"

	"github.com/gin-gonic/gin"
	gorillaWS "github.com/gorilla/websocket"
)

func main() {
	// 1. 连接 MySQL
	dsn := "root:@tcp(127.0.0.1:3306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := persistence.InitDB(dsn)
	if err != nil {
		log.Fatal("❌ MySQL 连接失败:", err)
	}
	repo := persistence.NewTradeRepo(db)
	balanceRepo := persistence.NewBalanceRepo(db)
	ledgerRepo := persistence.NewLedgerRepo(db)

	// 2. 创建引擎（支持多交易对）
	eng := engine.NewEngine()

	// 3. ✅ 初始化 Redis（用于快照恢复）
	rdb := engine.NewRedisClient("localhost:6379", "", 0)

	// 4. ✅ 为每个交易对初始化 Snapshotter + 从 Redis/WAL 恢复订单簿
	symbols := []string{"BTC/USDT", "ETH/USDT"}
	for _, symbol := range symbols {
		ob := eng.GetOrderBook(symbol)
		snapshotter := engine.NewSnapshotter(rdb, ob, db, symbol)

		// ✅ 启动时恢复：快照 → WAL → MySQL 对账
		if err := snapshotter.Load(); err != nil {
			log.Printf("⚠️ %s 订单簿恢复失败: %v", symbol, err)
		}

		// ✅ 启动定时快照（每 5 秒）
		snapshotter.StartAutoSave()
	}

	// 5. WebSocket Hub
	hub := websocket.NewHub()
	go hub.Run()

	var upgrader = gorillaWS.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// 6. 初始化 Service 和 Handler
	// ✅ 传入 balanceRepo
	orderService := service.NewOrderService(eng, repo, ledgerRepo, hub, balanceRepo)
	orderHandler := handler.NewOrderHandler(orderService)
	balanceHandler := handler.NewBalanceHandler(balanceRepo)
	depthHandler := handler.NewDepthHandler(eng)
	ledgerHandler := handler.NewLedgerHandler(ledgerRepo)

	// 7. 设置 Gin 路由
	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// WebSocket 路由
	r.GET("/ws", func(c *gin.Context) {
		userID := c.Query("user_id")
		if userID == "" {
			c.JSON(400, gin.H{"error": "user_id required"})
			return
		}

		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Println("WebSocket upgrade error:", err)
			return
		}

		client := &websocket.Client{
			Hub:    hub,
			Conn:   conn,
			Send:   make(chan []byte, 256),
			UserID: userID,
		}

		hub.Register <- client
		go client.WritePump()
		go client.ReadPump()
	})

	// 根路由
	r.GET("/", func(c *gin.Context) {
		c.String(200, "CEX API 运行中")
	})

	// API 路由
	api := r.Group("/api/v1")
	{
		api.POST("/orders", orderHandler.CreateOrder)
		api.GET("/orders/:id", orderHandler.GetOrder)
		api.GET("/orders/user/:user_id", orderHandler.GetOrdersByUser)
		api.DELETE("/orders/:id", orderHandler.CancelOrder)

		api.GET("/balance/:user_id/:asset", balanceHandler.GetBalance)
		api.POST("/balance/add", balanceHandler.AddBalance)
		api.POST("/balance/deduct", balanceHandler.DeductBalance)

		api.GET("/depth", depthHandler.GetDepth)

		api.GET("/ledger/:user_id", ledgerHandler.GetLedgerByUser)
	}

	fmt.Println("🚀 CEX API 启动在 http://localhost:8080")
	r.Run(":8080")
}
