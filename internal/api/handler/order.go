package handler

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cex-backend/internal/api/service"
	"cex-backend/internal/persistence"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var orderCreatedTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cex_orders_created_total",
		Help: "Total number of order creation attempts by result.",
	},
	[]string{"status"},
)

type OrderHandler struct {
	orderService *service.OrderService
}

func NewOrderHandler(orderService *service.OrderService) *OrderHandler {
	return &OrderHandler{
		orderService: orderService,
	}
}

// CreateOrder 下单
func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req service.CreateOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		orderCreatedTotal.WithLabelValues("failure").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}

	resp, err := h.orderService.CreateOrder(&req)
	if err != nil {
		orderCreatedTotal.WithLabelValues("failure").Inc()
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	orderCreatedTotal.WithLabelValues("success").Inc()
	c.JSON(http.StatusOK, resp)
}

// GetOrder 查询订单
func (h *OrderHandler) GetOrder(c *gin.Context) {
	orderID := c.Param("id")

	order, err := h.orderService.GetOrder(orderID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "订单不存在"})
		return
	}

	c.JSON(http.StatusOK, order)
}

// GetOrdersByUser 查询用户订单列表
func (h *OrderHandler) GetOrdersByUser(c *gin.Context) {
	userID := c.Param("user_id")

	orders, err := h.orderService.GetOrdersByUser(userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id": userID,
		"orders":  orders,
	})
}

type orderHistoryItem struct {
	OrderID     string    `json:"order_id"`
	UserID      string    `json:"user_id"`
	Symbol      string    `json:"symbol"`
	Side        string    `json:"side"`
	OrderType   string    `json:"order_type"`
	TimeInForce string    `json:"time_in_force"`
	Price       int64     `json:"price"`
	Amount      int64     `json:"amount"`
	Remaining   int64     `json:"remaining"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

var validOrderStatuses = map[string]struct{}{
	persistence.OrderStatusNew:             {},
	persistence.OrderStatusPartiallyFilled: {},
	persistence.OrderStatusFilled:          {},
	persistence.OrderStatusCanceled:        {},
	persistence.OrderStatusExpired:         {},
}

// GetOrders 查询用户订单，支持 status 逗号分隔过滤
func (h *OrderHandler) GetOrders(c *gin.Context) {
	userID := strings.TrimSpace(c.Query("user_id"))
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 不能为空"})
		return
	}

	statuses, err := parseStatuses(c.Query("status"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	limit := parseLimit(c.Query("limit"), 100)
	orders, err := h.orderService.GetOrdersByUserAndStatuses(userID, statuses, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	items := make([]orderHistoryItem, 0, len(orders))
	for _, o := range orders {
		items = append(items, orderHistoryItem{
			OrderID:     o.OrderID,
			UserID:      o.UserID,
			Symbol:      o.Symbol,
			Side:        o.Side,
			OrderType:   o.OrderType,
			TimeInForce: o.TimeInForce,
			Price:       o.Price,
			Amount:      o.Amount,
			Remaining:   o.Remaining,
			Status:      o.Status,
			CreatedAt:   o.CreatedAt,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id": userID,
		"orders":  items,
	})
}

// GetTrades 查询用户成交明细
func (h *OrderHandler) GetTrades(c *gin.Context) {
	userID := strings.TrimSpace(c.Query("user_id"))
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 不能为空"})
		return
	}

	limit := parseLimit(c.Query("limit"), 100)
	trades, err := h.orderService.GetTradesByUser(userID, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id": userID,
		"trades":  trades,
	})
}

func parseStatuses(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	parts := strings.Split(raw, ",")
	statuses := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		status := strings.ToUpper(strings.TrimSpace(part))
		if status == "" {
			continue
		}
		if _, ok := validOrderStatuses[status]; !ok {
			return nil, fmt.Errorf("无效的订单状态: %s", status)
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

func parseLimit(raw string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > 500 {
		return 500
	}
	return n
}

// CancelOrder 撤销订单
func (h *OrderHandler) CancelOrder(c *gin.Context) {
	orderID := c.Param("id")
	userID := c.Query("user_id")

	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 不能为空"})
		return
	}

	if err := h.orderService.CancelOrder(orderID, userID); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "订单已撤销"})
}
