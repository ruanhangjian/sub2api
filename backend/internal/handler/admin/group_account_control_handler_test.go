package admin

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type handlerGroupAccountControlRepo struct {
	result    *service.GroupAccountControl
	state     string
	confirmed bool
}

func (r *handlerGroupAccountControlRepo) Get(context.Context, int64, int64) (*service.GroupAccountControl, error) {
	return r.result, nil
}

func (r *handlerGroupAccountControlRepo) SetState(_ context.Context, _, _ int64, state string, confirmed bool) (*service.GroupAccountControl, error) {
	r.state, r.confirmed = state, confirmed
	return r.result, nil
}

func newGroupAccountControlHandlerRouter(repo *handlerGroupAccountControlRepo) *gin.Engine {
	gin.SetMode(gin.TestMode)
	h := NewGroupAccountControlHandler(service.NewGroupAccountControlService(repo))
	r := gin.New()
	r.GET("/internal/:group_id/:account_id", h.Get)
	r.PUT("/internal/:group_id/:account_id", h.Set)
	return r
}

func TestGroupAccountControlHandlerGet(t *testing.T) {
	repo := &handlerGroupAccountControlRepo{result: &service.GroupAccountControl{
		AccountID: 1,
		GroupID:   2,
		State:     service.GroupAccountStatePaused,
		Member:    true,
	}}
	r := newGroupAccountControlHandlerRouter(repo)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/internal/2/1", nil)
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"state":"paused"`)
	require.Contains(t, w.Body.String(), `"member":true`)
}

func TestGroupAccountControlHandlerSet(t *testing.T) {
	repo := &handlerGroupAccountControlRepo{result: &service.GroupAccountControl{State: service.GroupAccountStateRemoved}}
	r := newGroupAccountControlHandlerRouter(repo)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/internal/2/1", bytes.NewBufferString(`{"state":"removed","confirm_last_available":true}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, service.GroupAccountStateRemoved, repo.state)
	require.True(t, repo.confirmed)
}

func TestGroupAccountControlHandlerRejectsInvalidState(t *testing.T) {
	r := newGroupAccountControlHandlerRouter(&handlerGroupAccountControlRepo{})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/internal/2/1", bytes.NewBufferString(`{"state":"disabled"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
}
