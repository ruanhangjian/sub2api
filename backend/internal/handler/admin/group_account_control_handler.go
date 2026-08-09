package admin

import (
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type GroupAccountControlHandler struct {
	service *service.GroupAccountControlService
}

func NewGroupAccountControlHandler(svc *service.GroupAccountControlService) *GroupAccountControlHandler {
	return &GroupAccountControlHandler{service: svc}
}

type setGroupAccountStateRequest struct {
	State                string `json:"state" binding:"required,oneof=active paused removed"`
	ConfirmLastAvailable bool   `json:"confirm_last_available"`
}

// Get returns an account's scheduling state in one group.
func (h *GroupAccountControlHandler) Get(c *gin.Context) {
	accountID, groupID, ok := parseGroupAccountIDs(c)
	if !ok {
		return
	}
	result, err := h.service.Get(c.Request.Context(), accountID, groupID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

// Set switches a membership among active, paused and removed.
func (h *GroupAccountControlHandler) Set(c *gin.Context) {
	accountID, groupID, ok := parseGroupAccountIDs(c)
	if !ok {
		return
	}
	var req setGroupAccountStateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	result, err := h.service.SetState(c.Request.Context(), accountID, groupID, req.State, req.ConfirmLastAvailable)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func parseGroupAccountIDs(c *gin.Context) (accountID, groupID int64, ok bool) {
	var err error
	groupParam := c.Param("group_id")
	if groupParam == "" {
		groupParam = c.Param("id")
	}
	groupID, err = strconv.ParseInt(groupParam, 10, 64)
	if err != nil || groupID <= 0 {
		response.BadRequest(c, "Invalid group ID")
		return 0, 0, false
	}
	accountID, err = strconv.ParseInt(c.Param("account_id"), 10, 64)
	if err != nil || accountID <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return 0, 0, false
	}
	return accountID, groupID, true
}
