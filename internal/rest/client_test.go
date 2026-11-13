package rest

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
)

type fakeTokens struct {
	current     string
	invalidated int
}

func (f *fakeTokens) Token(context.Context) (string, error) {
	return f.current, nil
}

func (f *fakeTokens) Invalidate() {
	f.invalidated++
	if f.current == "token-1" {
		f.current = "token-2"
	}
}

func TestClientSendRefreshesUnauthorizedAndBuildsMarkdown(t *testing.T) {
	t.Parallel()

	tokens := &fakeTokens{current: "token-1"}
	var gotAuth string
	var gotPath string
	var gotBody map[string]any
	hits := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		hits++
		if hits == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":    40101,
				"message": "expired",
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":        "reply-1",
			"timestamp": "2026-03-29T14:00:00Z",
		})
	}))
	defer server.Close()

	client := New(Config{
		BaseURL:     server.URL,
		Client:      server.Client(),
		Tokens:      tokens,
		RetryCount:  0,
		RetryDelay:  time.Millisecond,
		DefaultKind: "text",
	})

	result, err := client.Send(context.Background(), SendRequest{
		RecipientKind: "group",
		RecipientID:   "group-1",
		ContentKind:   "markdown",
		Content:       "**ok**",
		Reply: &ReplyRequest{
			MessageID: "msg-1",
			Sequence:  2,
		},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if tokens.invalidated != 1 {
		t.Fatalf("invalidated = %d, want 1", tokens.invalidated)
	}
	if gotAuth != "QQBot token-2" {
		t.Fatalf("authorization = %q, want %q", gotAuth, "QQBot token-2")
	}
	if gotPath != "/v2/groups/group-1/messages" {
		t.Fatalf("path = %q, want %q", gotPath, "/v2/groups/group-1/messages")
	}
	if got, ok := gotBody["msg_type"].(float64); !ok || int(got) != 2 {
		t.Fatalf("msg_type = %#v, want 2", gotBody["msg_type"])
	}
	if gotBody["msg_id"] != "msg-1" {
		t.Fatalf("msg_id = %#v, want %#v", gotBody["msg_id"], "msg-1")
	}
	if got, ok := gotBody["msg_seq"].(float64); !ok || int(got) != 2 {
		t.Fatalf("msg_seq = %#v, want 2", gotBody["msg_seq"])
	}
	if result.ID != "reply-1" {
		t.Fatalf("result.ID = %q, want %q", result.ID, "reply-1")
	}
}

type alwaysInvalidTokens struct {
	invalidated int
}

func (a *alwaysInvalidTokens) Token(context.Context) (string, error) {
	return "bad-token", nil
}

func (a *alwaysInvalidTokens) Invalidate() {
	a.invalidated++
}

func TestClientSendPersistentUnauthorizedBoundsRetry(t *testing.T) {
	t.Parallel()

	tokens := &alwaysInvalidTokens{}
	var hits atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("x-tps-trace-id", "trace-401")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code":    40101,
			"message": "still unauthorized",
		})
	}))
	defer server.Close()

	client := New(Config{
		BaseURL:     server.URL,
		Client:      server.Client(),
		Tokens:      tokens,
		RetryCount:  3,
		RetryDelay:  time.Millisecond,
		DefaultKind: "text",
	})

	_, err := client.Send(context.Background(), SendRequest{
		RecipientKind: "c2c",
		RecipientID:   "user-1",
		Content:       "hello",
	})
	if err == nil {
		t.Fatal("Send() error = nil, want unauthorized error")
	}

	var apiErr *apiError
	if !errors.As(err, &apiErr) {
		t.Fatalf("Send() error = %T (%v), want *apiError", err, err)
	}
	if apiErr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("StatusCode = %d, want %d", apiErr.StatusCode, http.StatusUnauthorized)
	}
	if apiErr.Code != 40101 || apiErr.Message != "still unauthorized" || apiErr.TraceID != "trace-401" {
		t.Fatalf("unexpected apiError = %+v", apiErr)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("http hits = %d, want 2", got)
	}
	if tokens.invalidated != 1 {
		t.Fatalf("tokens.invalidated = %d, want 1", tokens.invalidated)
	}
}

func TestClientGatewayUnauthorizedRefreshAndBoundedFailure(t *testing.T) {
	t.Parallel()

	t.Run("succeeds_on_second_attempt", func(t *testing.T) {
		t.Parallel()

		tokens := &fakeTokens{current: "token-1"}
		var hits atomic.Int32
		var lastAuth string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			lastAuth = r.Header.Get("Authorization")
			if hits.Add(1) == 1 {
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"code":    40101,
					"message": "expired",
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"url": "wss://gateway.example.test/ws",
			})
		}))
		defer server.Close()

		client := New(Config{
			BaseURL: server.URL,
			Client:  server.Client(),
			Tokens:  tokens,
		})

		wsURL, err := client.Gateway(context.Background())
		if err != nil {
			t.Fatalf("Gateway() error = %v", err)
		}
		if wsURL != "wss://gateway.example.test/ws" {
			t.Fatalf("Gateway() url = %q, want %q", wsURL, "wss://gateway.example.test/ws")
		}
		if got := hits.Load(); got != 2 {
			t.Fatalf("http hits = %d, want 2", got)
		}
		if tokens.invalidated != 1 {
			t.Fatalf("tokens.invalidated = %d, want 1", tokens.invalidated)
		}
		if lastAuth != "QQBot token-2" {
			t.Fatalf("lastAuth = %q, want %q", lastAuth, "QQBot token-2")
		}
	})

	t.Run("fails_cleanly_when_401_persists", func(t *testing.T) {
		t.Parallel()

		tokens := &alwaysInvalidTokens{}
		var hits atomic.Int32

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("x-tps-trace-id", "gw-trace")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":    40102,
				"message": "invalid app secret",
			})
		}))
		defer server.Close()

		client := New(Config{
			BaseURL: server.URL,
			Client:  server.Client(),
			Tokens:  tokens,
		})

		_, err := client.Gateway(context.Background())
		if err == nil {
			t.Fatal("Gateway() error = nil, want unauthorized error")
		}

		var apiErr *apiError
		if !errors.As(err, &apiErr) {
			t.Fatalf("Gateway() error = %T (%v), want *apiError", err, err)
		}
		if apiErr.StatusCode != http.StatusUnauthorized || apiErr.Code != 40102 || apiErr.TraceID != "gw-trace" {
			t.Fatalf("unexpected apiError = %+v", apiErr)
		}
		if got := hits.Load(); got != 2 {
			t.Fatalf("http hits = %d, want 2", got)
		}
		if tokens.invalidated != 1 {
			t.Fatalf("tokens.invalidated = %d, want 1", tokens.invalidated)
		}
	})
}

func TestClientRecipientIDPathEscaping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		recipientKind string
		recipientID   string
		wantURI       string
	}{
		{
			name:          "c2c_traversal_and_slashes",
			recipientKind: "c2c",
			recipientID:   "../../admin",
			wantURI:       "/v2/users/..%2F..%2Fadmin/messages",
		},
		{
			name:          "group_query_and_fragment_chars",
			recipientKind: "group",
			recipientID:   "group/1?admin=1#frag",
			wantURI:       "/v2/groups/group%2F1%3Fadmin=1%23frag/messages",
		},
		{
			name:          "channel_dot_dot_segment",
			recipientKind: "channel",
			recipientID:   "..",
			wantURI:       "/channels/%2E%2E/messages",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var gotRequestURI string
			var gotRawQuery string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotRequestURI = r.RequestURI
				gotRawQuery = r.URL.RawQuery
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id":        "msg-ok",
					"timestamp": "2026-03-29T14:00:00Z",
				})
			}))
			defer server.Close()

			client := New(Config{
				BaseURL: server.URL,
				Client:  server.Client(),
				Tokens:  &fakeTokens{current: "token-1"},
			})

			_, err := client.Send(context.Background(), SendRequest{
				RecipientKind: tc.recipientKind,
				RecipientID:   tc.recipientID,
				Content:       "hello",
			})
			if err != nil {
				t.Fatalf("Send() error = %v", err)
			}
			if gotRequestURI != tc.wantURI {
				t.Fatalf("RequestURI = %q, want %q", gotRequestURI, tc.wantURI)
			}
			if gotRawQuery != "" {
				t.Fatalf("RawQuery = %q, want empty", gotRawQuery)
			}
		})
	}
}

func TestClientDrainsResponseBodyForKeepAliveReuse(t *testing.T) {
	t.Parallel()

	var newConns atomic.Int32
	tokens := &fakeTokens{current: "token-1"}
	var reqCount atomic.Int32

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqCount.Add(1) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":40101,"message":"` + strings.Repeat("x", 4096) + `"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"ok","timestamp":"2026-03-29T14:00:00Z"}`))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	client := New(Config{
		BaseURL: server.URL,
		Client:  server.Client(),
		Tokens:  tokens,
	})

	if _, err := client.Send(context.Background(), SendRequest{
		RecipientKind: "c2c",
		RecipientID:   "user-1",
		Content:       "ping",
	}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if got := newConns.Load(); got != 1 {
		t.Fatalf("new TCP connections = %d, want 1 (keep-alive reused)", got)
	}
}
