package service

import (
	"context"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

// subPilotClientSingleton 是进程级共享的 SubPilot HTTP 客户端。
// 在第一次调用时惰性构造；disabled 模式下永远不会被使用。
var subPilotClientSingleton = NewSubPilotClient()

// subPilotConfig 解析当前服务配置下的 SubPilot 接入配置。
// 返回零值（Enabled=false）表示关闭，调用方据此短路。
func subPilotConfig(cfg *config.Config) config.SubPilotConfig {
	if cfg == nil {
		return config.SubPilotConfig{}
	}
	sp := cfg.Gateway.SubPilot
	if sp.TimeoutMS <= 0 {
		sp.TimeoutMS = 80
	}
	// FailOpen 默认 true：未显式配置时按 fail-open 处理，绝不阻塞用户请求。
	if !sp.Enabled {
		// disabled 时保持零值，避免误用。
		return config.SubPilotConfig{Enabled: false}
	}
	return sp
}

// trySubPilotRecommendForGateway 是 Claude/Anthropic 路径的 SubPilot 推荐入口。
//
// 它在原生账号选择之前调用 SubPilot 获取推荐账号；若拿到合法推荐，
// 则在 Sub2API 内部做完整二次校验（group / platform / status / schedulable /
// model / excludedIDs）后，按原生方式获取并发槽位并注册会话，返回可用的
// AccountSelectionResult。
//
// 任何环节失败（disabled / 超时 / 网络错误 / SubPilot 未选中 / 二次校验失败 /
// 槽位获取失败 / 会话限制）都返回 nil，由调用方 fail-open 回原生调度。
//
// 严格保证：绝不绕过 Sub2API 原有的 group / model / platform / status /
// concurrency / session 权限逻辑——推荐账号必须通过全部原生校验才会被使用。
func (s *GatewayService) trySubPilotRecommendForGateway(
	ctx context.Context,
	groupID *int64,
	sessionHash string,
	requestedModel string,
	excludedIDs map[int64]struct{},
) *AccountSelectionResult {
	sp := subPilotConfig(s.cfg)
	if !sp.Enabled || sp.BaseURL == "" {
		return nil
	}
	if groupID == nil {
		return nil
	}

	platform := s.gatewayPlatform(ctx, groupID)
	recommend, err := subPilotClientSingleton.RecommendAccount(ctx, sp, subpilotRecommendRequest{
		RequestID:  requestIDFromContext(ctx),
		Platform:   platformForSubPilot(platform),
		GroupID:    groupIDToString(groupID),
		Model:      requestedModel,
		SessionKey: sessionHash,
	})
	if err != nil || recommend == nil {
		return nil // fail-open
	}
	if _, excluded := excludedIDs[recommend.AccountID]; excluded {
		return nil // 推荐账号在排除列表里 → fail-open
	}

	// 二次校验：确认推荐账号确实属于该分组、平台匹配、可调度。
	account, ok := s.validateSubPilotAccount(ctx, *groupID, platform, recommend.AccountID)
	if !ok {
		return nil
	}

	// 按原生方式获取并发槽位。
	result, err := s.tryAcquireAccountSlot(ctx, account.ID, account.Concurrency)
	if err != nil || !result.Acquired {
		return nil // 槽位满 → fail-open（让原生调度去排队或选别的）
	}

	// 会话限制检查（与原生调度一致）。
	if !s.checkAndRegisterSession(ctx, account, sessionHash) {
		result.ReleaseFunc()
		return nil // 会话限制 → fail-open
	}

	selection, err := s.newSelectionResult(ctx, account, true, result.ReleaseFunc, nil)
	if err != nil {
		result.ReleaseFunc()
		return nil
	}
	// 问题1：把 SubPilot /select 返回的 lease_id 带回 selection，
	// 供调用方 handler 写回 ctx，使后续 report 能释放 lease。
	selection.SubPilotLeaseID = recommend.LeaseID
	return selection
}

// validateSubPilotAccount 校验 SubPilot 推荐的账号是否可被 Sub2API 合法使用。
// 通过 ListSchedulableByGroupIDAndPlatform 取该分组该平台下的可调度账号池，
// 推荐账号必须在池中。这同时校验了：group 归属、platform 匹配、status=active、
// schedulable=true。不通过返回 (nil, false)。
func (s *GatewayService) validateSubPilotAccount(ctx context.Context, groupID int64, platform string, accountID int64) (*Account, bool) {
	accounts, err := s.accountRepo.ListSchedulableByGroupIDAndPlatform(ctx, groupID, platform)
	if err != nil {
		return nil, false
	}
	for i := range accounts {
		if accounts[i].ID == accountID {
			return &accounts[i], true
		}
	}
	return nil, false
}

// trySubPilotRecommendForOpenAI 是 OpenAI/ChatGPT 路径的 SubPilot 推荐入口。
// 逻辑与 Claude 版本对称：问 SubPilot 拿推荐 → 二次校验 → 获取并发槽位。
// OpenAI 路径不做会话注册检查（与原生 selectAccountWithLoadAwareness 一致）。
// 任何失败都返回 nil，由调用方 fail-open。
func (s *OpenAIGatewayService) trySubPilotRecommendForOpenAI(
	ctx context.Context,
	groupID *int64,
	sessionHash string,
	requestedModel string,
	excludedIDs map[int64]struct{},
) *AccountSelectionResult {
	sp := subPilotConfig(s.cfg)
	if !sp.Enabled || sp.BaseURL == "" {
		return nil
	}
	if groupID == nil {
		return nil
	}

	recommend, err := subPilotClientSingleton.RecommendAccount(ctx, sp, subpilotRecommendRequest{
		RequestID:  requestIDFromContext(ctx),
		Platform:   platformForSubPilot(PlatformOpenAI),
		GroupID:    groupIDToString(groupID),
		Model:      requestedModel,
		SessionKey: sessionHash,
	})
	if err != nil || recommend == nil {
		return nil
	}
	if _, excluded := excludedIDs[recommend.AccountID]; excluded {
		return nil
	}

	account, ok := s.validateSubPilotAccountOpenAI(ctx, *groupID, recommend.AccountID)
	if !ok {
		return nil
	}

	result, err := s.tryAcquireAccountSlot(ctx, account.ID, account.Concurrency)
	if err != nil || result == nil || !result.Acquired {
		return nil
	}

	selection, err := s.newAcquiredSelectionResult(ctx, account, result.ReleaseFunc)
	if err != nil {
		result.ReleaseFunc()
		return nil
	}
	// 问题1：把 SubPilot /select 返回的 lease_id 带回 selection。
	selection.SubPilotLeaseID = recommend.LeaseID
	return selection
}

// validateSubPilotAccountOpenAI 校验推荐账号属于该分组的 openai 可调度池。
func (s *OpenAIGatewayService) validateSubPilotAccountOpenAI(ctx context.Context, groupID int64, accountID int64) (*Account, bool) {
	accounts, err := s.accountRepo.ListSchedulableByGroupIDAndPlatform(ctx, groupID, PlatformOpenAI)
	if err != nil {
		return nil, false
	}
	for i := range accounts {
		if accounts[i].ID == accountID {
			return &accounts[i], true
		}
	}
	return nil, false
}

// gatewayPlatform 解析分组对应的平台。
func (s *GatewayService) gatewayPlatform(ctx context.Context, groupID *int64) string {
	if groupID == nil {
		return PlatformAnthropic
	}
	group, _, err := s.resolveGatewayGroup(ctx, groupID)
	if err != nil || group == nil {
		return PlatformAnthropic
	}
	return group.Platform
}

// requestIDFromContext 从 ctx 里提取请求 ID（用于 SubPilot lease 追踪），缺失时返回空。
func requestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(ctxkey.RequestID).(string); ok {
		return v
	}
	return ""
}

// groupIDToString 把 *int64 groupID 转字符串。
func groupIDToString(groupID *int64) string {
	if groupID == nil {
		return ""
	}
	return strconv.FormatInt(*groupID, 10)
}
