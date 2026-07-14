package handler

import (
	"net/http"

	"cex-backend/internal/persistence"

	"github.com/gin-gonic/gin"
)

type BalanceHandler struct {
	repo *persistence.BalanceRepo
}

func NewBalanceHandler(repo *persistence.BalanceRepo) *BalanceHandler {
	return &BalanceHandler{repo: repo}
}

// GetBalance 查询用户余额
func (h *BalanceHandler) GetBalance(c *gin.Context) {
	userID := c.Param("user_id")
	asset := c.Param("asset")

	bal, err := h.repo.GetBalance(userID, asset)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id":   bal.UserID,
		"asset":     bal.Asset,
		"available": bal.Available,
		"frozen":    bal.Frozen,
		"total":     bal.Total,
	})
}

// AddBalance 给用户充值（测试用）
func (h *BalanceHandler) AddBalance(c *gin.Context) {
	var req struct {
		UserID string  `json:"user_id"`
		Asset  string  `json:"asset"`
		Amount float64 `json:"amount"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请求格式错误"})
		return
	}

	if err := h.repo.AddBalance(req.UserID, req.Asset, req.Amount); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "充值成功"})
}

// DeductBalance 扣减余额（测试用）
func (h *BalanceHandler) DeductBalance(c *gin.Context) {
	var req struct {
		UserID string  `json:"user_id"`
		Asset  string  `json:"asset"`
		Amount float64 `json:"amount"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "请求格式错误"})
		return
	}

	if err := h.repo.DeductBalance(req.UserID, req.Asset, req.Amount); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}

	c.JSON(200, gin.H{"message": "扣减成功"})
}
