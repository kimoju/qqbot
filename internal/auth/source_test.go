package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSourceCachesToken(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "token-1",
			"expires_in":   7200,
		})
	}))
	defer server.Close()

	source, err := New(Config{
		AppID:     "app-id",
		AppSecret: "secret",
		Endpoint:  server.URL,
		Client:    server.Client(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx := context.Background()
	first, err := source.Token(ctx)
	if err != nil {
		t.Fatalf("Token() first error = %v", err)
	}
	second, err := source.Token(ctx)
	if err != nil {
		t.Fatalf("Token() second error = %v", err)
	}

	if first != "token-1" || second != "token-1" {
		t.Fatalf("unexpected tokens: %q %q", first, second)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
}

func TestSourceCachesShortLivedTokensWithoutRefreshStorm(t *testing.T) {
	t.Parallel()

	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			// 240s (<= 300s): previously triggered a refresh storm on every single call!
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "short-240",
				"expires_in":   240,
			})
		case 2:
			// 90s: adaptive lead = min(45s, 90s/3) = 30s -> cached for 60s.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "short-90",
				"expires_in":   90,
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "fresh-7200",
				"expires_in":   7200,
			})
		}
	}))
	defer server.Close()

	source, err := New(Config{
		AppID:     "app-id",
		AppSecret: "secret",
		Endpoint:  server.URL,
		Client:    server.Client(),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var clockMu sync.Mutex
	curTime := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	source.now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return curTime
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		curTime = curTime.Add(d)
		clockMu.Unlock()
	}

	ctx := context.Background()

	// For expires_in=240s, lead=min(45s, 80s)=45s -> cached until +195s.
	for step := 0; step < 19; step++ {
		tok, err := source.Token(ctx)
		if err != nil {
			t.Fatalf("Token() step %d error = %v", step, err)
		}
		if tok != "short-240" {
			t.Fatalf("step %d token = %q, want short-240", step, tok)
		}
		advance(10 * time.Second)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("hits during 190s of 240s token = %d, want 1 (no refresh storm)", got)
	}

	// Advance past 195s (to 200s -> 40s remaining <= 45s lead) -> refreshes once!
	advance(10 * time.Second)
	tok, err := source.Token(ctx)
	if err != nil {
		t.Fatalf("Token() refresh error = %v", err)
	}
	if tok != "short-90" {
		t.Fatalf("token after refresh = %q, want short-90", tok)
	}
	if got := hits.Load(); got != 2 {
		t.Fatalf("hits = %d, want 2", got)
	}

	// For expires_in=90s, lead=min(45s, 30s)=30s -> cached until +60s.
	advance(59 * time.Second)
	tok, err = source.Token(ctx)
	if err != nil {
		t.Fatalf("Token() at 59s error = %v", err)
	}
	if tok != "short-90" || hits.Load() != 2 {
		t.Fatalf("at 59s of 90s token: tok=%q hits=%d, want short-90 and 2", tok, hits.Load())
	}

	// At +60s (30s remaining <= 30s lead), refreshes cleanly to fresh-7200.
	advance(1 * time.Second)
	tok, err = source.Token(ctx)
	if err != nil {
		t.Fatalf("Token() at 60s error = %v", err)
	}
	if tok != "fresh-7200" || hits.Load() != 3 {
		t.Fatalf("at 60s of 90s token: tok=%q hits=%d, want fresh-7200 and 3", tok, hits.Load())
	}

	// Explicit Invalidate() forces immediate refresh even with ~2h remaining.
	source.Invalidate()
	tok, err = source.Token(ctx)
	if err != nil {
		t.Fatalf("Token() after Invalidate error = %v", err)
	}
	if tok != "fresh-7200" || hits.Load() != 4 {
		t.Fatalf("after Invalidate: tok=%q hits=%d, want fresh-7200 and 4", tok, hits.Load())
	}
}
