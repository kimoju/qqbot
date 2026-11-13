package qqbot

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestAppCommandRespondsWithIncrementingSequenceAndDeduplicates(t *testing.T) {
	t.Parallel()

	var h *testHarness
	h = newTestHarness(t, func(conn *websocket.Conn) {
		defer conn.Close()
		writeFrame(t, conn, map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		})
		readIdentify(t, conn)

		dispatch := map[string]any{
			"op": 0,
			"s":  1,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "msg-1",
				"content":   "/status",
				"timestamp": "2026-03-29T15:00:00Z",
				"author": map[string]any{
					"user_openid": "user-1",
				},
			},
		}
		writeFrame(t, conn, dispatch)
		writeFrame(t, conn, dispatch)
		<-time.After(2 * time.Second)
	})
	defer h.Close()

	app, err := New(h.Settings())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var calls atomic.Int32
	app.Command("status", func(ctx context.Context, c Conversation, cmd ParsedCommand) error {
		calls.Add(1)
		if _, err := c.RespondText(ctx, "first"); err != nil {
			return err
		}
		_, err := c.RespondText(ctx, "second")
		return err
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx)
	}()

	first := h.WaitMessage(t)
	second := h.WaitMessage(t)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() timeout")
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("command calls = %d, want 1", got)
	}
	assertMessage(t, first, "/v2/users/user-1/messages", "QQBot token-1", "first", "msg-1", 1)
	assertMessage(t, second, "/v2/users/user-1/messages", "QQBot token-1", "second", "msg-1", 2)
}

func TestAppUnknownCommandUsesDefaultReply(t *testing.T) {
	t.Parallel()

	var h *testHarness
	h = newTestHarness(t, func(conn *websocket.Conn) {
		defer conn.Close()
		writeFrame(t, conn, map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		})
		readIdentify(t, conn)
		writeFrame(t, conn, map[string]any{
			"op": 0,
			"s":  1,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "msg-unknown",
				"content":   "/missing",
				"timestamp": "2026-03-29T15:10:00Z",
				"author": map[string]any{
					"user_openid": "user-2",
				},
			},
		})
		<-time.After(2 * time.Second)
	})
	defer h.Close()

	app, err := New(h.Settings())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx)
	}()

	msg := h.WaitMessage(t)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() timeout")
	}

	assertMessage(t, msg, "/v2/users/user-2/messages", "QQBot token-1", "未知命令", "msg-unknown", 1)
}

func TestAppOnTextHandlesNonCommand(t *testing.T) {
	t.Parallel()

	var h *testHarness
	h = newTestHarness(t, func(conn *websocket.Conn) {
		defer conn.Close()
		writeFrame(t, conn, map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		})
		readIdentify(t, conn)
		writeFrame(t, conn, map[string]any{
			"op": 0,
			"s":  1,
			"t":  "GROUP_AT_MESSAGE_CREATE",
			"d": map[string]any{
				"id":           "msg-text",
				"content":      "<@bot> hello",
				"timestamp":    "2026-03-29T15:20:00Z",
				"group_openid": "group-9",
				"author": map[string]any{
					"member_openid": "member-9",
				},
			},
		})
		<-time.After(2 * time.Second)
	})
	defer h.Close()

	app, err := New(h.Settings())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	app.OnText(func(ctx context.Context, c Conversation) error {
		_, err := c.RespondText(ctx, "text-handler")
		return err
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx)
	}()

	msg := h.WaitMessage(t)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() timeout")
	}

	assertMessage(t, msg, "/v2/groups/group-9/messages", "QQBot token-1", "text-handler", "msg-text", 1)
}

func TestAppCloseCleansUpOwnedHTTPClientAndPreservesExternalClient(t *testing.T) {
	t.Parallel()

	newConnTrackingServer := func(t *testing.T) (*httptest.Server, <-chan struct{}, <-chan struct{}) {
		t.Helper()
		idleCh := make(chan struct{}, 8)
		closedCh := make(chan struct{}, 8)

		mux := http.NewServeMux()
		mux.HandleFunc("/app/getAppAccessToken", func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "token-1",
				"expires_in":   7200,
			})
		})
		mux.HandleFunc("/v2/users/", func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":        "reply-1",
				"timestamp": "2026-03-29T15:01:00Z",
			})
		})

		srv := httptest.NewUnstartedServer(mux)
		srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			switch state {
			case http.StateIdle:
				select {
				case idleCh <- struct{}{}:
				default:
				}
			case http.StateClosed:
				select {
				case closedCh <- struct{}{}:
				default:
				}
			}
		}
		srv.Start()
		t.Cleanup(srv.Close)
		return srv, idleCh, closedCh
	}

	t.Run("closes_idle_connections_when_http_client_is_owned", func(t *testing.T) {
		t.Parallel()

		srv, idleCh, closedCh := newConnTrackingServer(t)

		app, err := New(Settings{
			AppID:         "app-id",
			AppSecret:     "secret",
			TokenEndpoint: srv.URL + "/app/getAppAccessToken",
			BaseEndpoint:  srv.URL,
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if !app.ownHTTPClient {
			t.Fatal("ownHTTPClient = false, want true")
		}

		if _, err := app.Send(context.Background(), Delivery{
			To:   Recipient{Kind: DirectRecipient, ID: "user-1"},
			Body: "hello",
		}); err != nil {
			t.Fatalf("Send() error = %v", err)
		}

		select {
		case <-idleCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for keep-alive connection to become idle")
		}

		// Before Close(), the idle connection should remain open.
		select {
		case <-closedCh:
			t.Fatal("idle connection closed prematurely before App.Close()")
		case <-time.After(30 * time.Millisecond):
		}

		if err := app.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}

		select {
		case <-closedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for App.Close() to close idle HTTP connection")
		}
	})

	t.Run("does_not_close_caller_supplied_http_client_idle_connections", func(t *testing.T) {
		t.Parallel()

		srv, idleCh, closedCh := newConnTrackingServer(t)
		customClient := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        16,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
			},
		}
		t.Cleanup(customClient.CloseIdleConnections)

		app, err := New(Settings{
			AppID:         "app-id",
			AppSecret:     "secret",
			TokenEndpoint: srv.URL + "/app/getAppAccessToken",
			BaseEndpoint:  srv.URL,
			HTTPClient:    customClient,
		})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if app.ownHTTPClient {
			t.Fatal("ownHTTPClient = true, want false")
		}

		if _, err := app.Send(context.Background(), Delivery{
			To:   Recipient{Kind: DirectRecipient, ID: "user-1"},
			Body: "hello",
		}); err != nil {
			t.Fatalf("Send() error = %v", err)
		}

		select {
		case <-idleCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for keep-alive connection to become idle")
		}

		if err := app.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}

		// Caller-owned HTTPClient's idle socket must remain open after App.Close().
		select {
		case <-closedCh:
			t.Fatal("caller-supplied HTTPClient connection was unexpectedly closed by App.Close()")
		case <-time.After(50 * time.Millisecond):
		}

		customClient.CloseIdleConnections()
		select {
		case <-closedCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for customClient.CloseIdleConnections()")
		}
	})
}

func TestAppContinuesAfterHandlerErrorAndStopsViaClose(t *testing.T) {
	t.Parallel()

	var wsConns atomic.Int32
	h := newTestHarness(t, func(conn *websocket.Conn) {
		defer conn.Close()
		wsConns.Add(1)
		writeFrame(t, conn, map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		})
		readIdentify(t, conn)
		writeFrame(t, conn, map[string]any{
			"op": 0,
			"s":  1,
			"t":  "READY",
			"d":  map[string]any{},
		})
		writeFrame(t, conn, map[string]any{
			"op": 0,
			"s":  2,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "msg-err",
				"content":   "/fail",
				"timestamp": "2026-03-29T15:00:00Z",
				"author":    map[string]any{"user_openid": "user-1"},
			},
		})
		writeFrame(t, conn, map[string]any{
			"op": 0,
			"s":  3,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "msg-next",
				"content":   "/ok",
				"timestamp": "2026-03-29T15:00:01Z",
				"author":    map[string]any{"user_openid": "user-1"},
			},
		})
		<-time.After(2 * time.Second)
	})
	defer h.Close()

	app, err := New(h.Settings())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	app.Command("fail", func(context.Context, Conversation, ParsedCommand) error {
		return errors.New("handler boom")
	})
	app.Command("ok", func(ctx context.Context, c Conversation, _ ParsedCommand) error {
		_, err := c.RespondText(ctx, "recovered")
		return err
	})

	done := make(chan error, 1)
	go func() {
		done <- app.Run(context.Background())
	}()

	msg := h.WaitMessage(t)
	if err := app.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() timeout after App.Close()")
	}

	if got := wsConns.Load(); got != 1 {
		t.Fatalf("wsConns = %d, want 1", got)
	}
	assertMessage(t, msg, "/v2/users/user-1/messages", "QQBot token-1", "recovered", "msg-next", 1)
}

type captured struct {
	Path string
	Auth string
	Body map[string]any
}

type testHarness struct {
	t        *testing.T
	server   *httptest.Server
	wsURL    string
	wsHandle func(*websocket.Conn)
	messages chan captured
}

func newTestHarness(t *testing.T, wsHandler func(*websocket.Conn)) *testHarness {
	t.Helper()

	h := &testHarness{
		t:        t,
		wsHandle: wsHandler,
		messages: make(chan captured, 8),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/app/getAppAccessToken", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "token-1",
			"expires_in":   7200,
		})
	})
	mux.HandleFunc("/gateway", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"url": h.wsURL,
		})
	})
	mux.HandleFunc("/v2/users/", h.capture)
	mux.HandleFunc("/v2/groups/", h.capture)
	mux.HandleFunc("/channels/", h.capture)
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		h.wsHandle(conn)
	})

	h.server = httptest.NewServer(mux)
	h.wsURL = "ws" + strings.TrimPrefix(h.server.URL, "http") + "/ws"
	return h
}

func (h *testHarness) Settings() Settings {
	return Settings{
		AppID:         "app-id",
		AppSecret:     "secret",
		TokenEndpoint: h.server.URL + "/app/getAppAccessToken",
		BaseEndpoint:  h.server.URL,
	}
}

func (h *testHarness) Close() {
	h.server.Close()
}

func (h *testHarness) WaitMessage(t *testing.T) captured {
	t.Helper()
	select {
	case msg := <-h.messages:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
		return captured{}
	}
}

func (h *testHarness) capture(w http.ResponseWriter, r *http.Request) {
	body := map[string]any{}
	_ = json.NewDecoder(r.Body).Decode(&body)
	h.messages <- captured{
		Path: r.URL.Path,
		Auth: r.Header.Get("Authorization"),
		Body: body,
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":        "reply-id",
		"timestamp": "2026-03-29T15:01:00Z",
	})
}

func writeFrame(t *testing.T, conn *websocket.Conn, payload any) {
	t.Helper()
	if err := conn.WriteJSON(payload); err != nil {
		t.Fatalf("WriteJSON() error = %v", err)
	}
}

func readIdentify(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage() error = %v", err)
	}
	frame := map[string]any{}
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if op, _ := frame["op"].(float64); int(op) != 2 {
		t.Fatalf("identify op = %#v, want 2", frame["op"])
	}
}

func assertMessage(t *testing.T, msg captured, path string, auth string, content string, msgID string, seq int) {
	t.Helper()
	if msg.Path != path {
		t.Fatalf("path = %q, want %q", msg.Path, path)
	}
	if msg.Auth != auth {
		t.Fatalf("auth = %q, want %q", msg.Auth, auth)
	}
	if msg.Body["content"] != content {
		t.Fatalf("content = %#v, want %#v", msg.Body["content"], content)
	}
	if msg.Body["msg_id"] != msgID {
		t.Fatalf("msg_id = %#v, want %#v", msg.Body["msg_id"], msgID)
	}
	if got, ok := msg.Body["msg_seq"].(float64); !ok || int(got) != seq {
		t.Fatalf("msg_seq = %#v, want %d", msg.Body["msg_seq"], seq)
	}
}
