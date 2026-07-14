package handler

import (
	"net/http"
	"strconv"

	"cex-backend/internal/engine"

	"github.com/gin-gonic/gin"
)

type DepthHandler struct {
	eng *engine.Engine
}

func NewDepthHandler(eng *engine.Engine) *DepthHandler {
	return &DepthHandler{eng: eng}
}

// GetDepth 获取订单簿深度
func (h *DepthHandler) GetDepth(c *gin.Context) {
	symbol := c.Query("symbol")
	if symbol == "" {
		symbol = "BTC/USDT"
	}

	limit := 10
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	depth := h.eng.GetDepth(symbol, limit)
	c.JSON(http.StatusOK, depth)
}
