package handler

import (
	"net/http"

	"cex-backend/internal/api/service"

	"github.com/gin-gonic/gin"
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}

	resp, err := h.orderService.CreateOrder(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

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
