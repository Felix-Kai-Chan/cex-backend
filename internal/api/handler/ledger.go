package handler

import (
	"net/http"

	"cex-backend/internal/persistence"

	"github.com/gin-gonic/gin"
)

type LedgerHandler struct {
	ledgerRepo *persistence.LedgerRepo
}

func NewLedgerHandler(ledgerRepo *persistence.LedgerRepo) *LedgerHandler {
	return &LedgerHandler{ledgerRepo: ledgerRepo}
}

// GetLedgerByUser 查询用户流水
func (h *LedgerHandler) GetLedgerByUser(c *gin.Context) {
	userID := c.Param("user_id")

	var entries []persistence.LedgerModel
	if err := h.ledgerRepo.GetLedgerByUser(userID, &entries); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id": userID,
		"ledgers": entries,
	})
}
