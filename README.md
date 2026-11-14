# qqbot

`qqbot`（`github.com/kimoju/qqbot`）是一个面向 Go 服务内嵌场景的轻量级 QQ 官方机器人 SDK，为 [VoHive](https://github.com/kimoju/vohive) 等常驻后台提供稳定的消息推送与双向命令交互能力。

## 核心特性

- **主动推送与被动回复统一封装**：支持单聊（C2C）、群聊（Group）、频道子频道（Channel）与频道私信（DMS）文本消息发送，同一会话内自动携带 `msg_id` 与递增 `msg_seq` 完成被动回复。
- **自适应 OAuth2 令牌管理**：内置 `AccessToken` 缓存与提前刷新策略（自适应 `min(45s, ttl/3)` 提前量），实现 `InvalidatableTokenProvider` 在遇到服务端 `401` 时自动失效重取并透明重试一次。
- **高可靠 WebSocket Gateway 状态机**：
  - 完整处理 `op: 10` Hello 心跳协商、`op: 1/11` Ping/Pong 读写超时续期与并发写锁保护；
  - 支持断线后优先发起 `op: 6` Resume 会话恢复，收到 `op: 7` Reconnect 或 `op: 9` Invalid Session 时自动按规范切换重连或重新 `Identify`；
  - 内置指数退避 + 随机抖动（Jitter）重连策略。
- **有界并发命令分发与防重放**：
  - 事件消费采用有界 Worker 池异步执行，避免慢命令阻塞 Gateway 心跳回路；
  - 内置容量有界的 `MemorySeenStore`（默认 `16384`）实现事件幂等去重；
  - 自动剥离群聊/频道消息中的单个或多个 `@机器人` 提及前缀并解析 `/command args`；
  - REST 请求自动识别 `429 Too Many Requests` 的 `Retry-After` 响应头并进行受控退避重试。
- **优雅生命周期管理**：`App.Run(ctx)` 与 `App.Close()` 协同等待底层连接与在途处理器安全退出。

## 安装

```bash
go get github.com/kimoju/qqbot@latest
```

## 快速上手

### 1. 主动发送消息

```go
package main

import (
	"context"

	"github.com/kimoju/qqbot"
)

func main() {
	app, err := qqbot.New(qqbot.Settings{
		AppID:     "你的 AppID",
		AppSecret: "你的 AppSecret",
	})
	if err != nil {
		panic(err)
	}
	defer app.Close()

	_, err = app.Send(context.Background(), qqbot.Delivery{
		To: qqbot.Recipient{
			Kind: qqbot.DirectRecipient,
			ID:   "user_openid",
		},
		Kind: qqbot.PlainText,
		Body: "hello from qqbot",
	})
	if err != nil {
		panic(err)
	}
}
```

### 2. 注册斜杠命令与常驻监听

```go
package main

import (
	"context"

	"github.com/kimoju/qqbot"
)

func main() {
	app, err := qqbot.New(qqbot.Settings{
		AppID:     "你的 AppID",
		AppSecret: "你的 AppSecret",
	})
	if err != nil {
		panic(err)
	}
	defer app.Close()

	app.Command("status", func(ctx context.Context, c qqbot.Conversation, cmd qqbot.ParsedCommand) error {
		_, err := c.RespondText(ctx, "服务运行正常")
		return err
	})

	app.OnText(func(ctx context.Context, c qqbot.Conversation) error {
		_, err := c.RespondText(ctx, "请发送 /status 查询状态")
		return err
	})

	_ = app.Run(context.Background())
}
```

## 核心入口一览

| API | 说明 |
| --- | --- |
| `qqbot.New(settings)` | 创建机器人实例并初始化 Token 与 HTTP/Gateway 客户端 |
| `App.Send(ctx, delivery)` | 向指定单聊/群聊/频道目标发送消息 |
| `App.Command(name, handler)` | 注册 `/name` 斜杠命令处理器（大小写不敏感，自动剥离 `@mention`） |
| `App.OnText(handler)` | 注册未命中斜杠命令时的普通文本兜底处理器 |
| `App.Run(ctx)` | 启动 WebSocket Gateway 事件循环并阻塞运行直至取消 |
| `App.Close()` | 主动关闭底层连接并等待运行循环安全退出 |
| `Conversation.Respond / RespondText` | 在当前入站消息上下文中按被动回复语义连续回消息 |

## 开发与测试

```bash
go vet ./...
go test -race -count=1 ./...
```
