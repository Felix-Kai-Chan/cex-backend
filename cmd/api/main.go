package main

import (
	"log/slog"
	"net/http"
	"os"

	"cex-backend/internal/api/handler"
	"cex-backend/internal/api/service"
	"cex-backend/internal/engine"
	"cex-backend/internal/persistence"
	"cex-backend/internal/websocket"

	"github.com/gin-gonic/gin"
	gorillaWS "github.com/gorilla/websocket"
)

func main() {
	// ✅ 初始化 slog（JSON 格式，INFO 级别）
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// 1. 连接 MySQL
	dsn := "root:@tcp(127.0.0.1:3306)/cex?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := persistence.InitDB(dsn)
	if err != nil {
		slog.Error("MySQL 连接失败", "error", err)
		os.Exit(1)
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
			slog.Warn("订单簿恢复失败", "symbol", symbol, "error", err)
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
	orderService := service.NewOrderService(eng, repo, ledgerRepo, hub, balanceRepo)
	orderHandler := handler.NewOrderHandler(orderService)
	balanceHandler := handler.NewBalanceHandler(balanceRepo)
	depthHandler := handler.NewDepthHandler(eng)
	ledgerHandler := handler.NewLedgerHandler(ledgerRepo)

	// 7. 设置 Gin 路由
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()         // ✅ 不用 Default()，避免 Gin 默认日志
	r.Use(gin.Recovery())  // ✅ 保留 panic 恢复
	r.Use(RequestLogger()) // ✅ 用 slog 记录请求

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
			slog.Error("WebSocket upgrade error", "error", err)
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

	slog.Info("CEX API 启动", "addr", "http://localhost:8080")
	r.Run(":8080")
}

// ✅ RequestLogger 用 slog 记录 HTTP 请求
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		slog.Info("request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", c.Writer.Size(),
			"client_ip", c.ClientIP(),
		)
	}
}
