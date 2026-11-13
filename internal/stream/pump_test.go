package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type staticLookup struct {
	url string
}

func (s staticLookup) Gateway(context.Context) (string, error) {
	return s.url, nil
}

type staticTokens struct {
	token string
}

func (s staticTokens) Token(context.Context) (string, error) {
	return s.token, nil
}

func newWSServer(t *testing.T, handler func(*websocket.Conn)) (*httptest.Server, string) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		handler(conn)
	}))
	t.Cleanup(srv.Close)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	return srv, wsURL
}

func TestPumpOp9BeforeReadyUsesExponentialBackoffAndResetsAfterReady(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sessions atomic.Int32
	_, wsURL := newWSServer(t, func(conn *websocket.Conn) {
		defer conn.Close()
		attempt := sessions.Add(1)

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		}); err != nil {
			return
		}
		// Read IDENTIFY (op: 2)
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}

		switch attempt {
		case 1, 2, 3:
			// Server rejects session BEFORE sending READY -> ready must stay false!
			_ = conn.WriteJSON(map[string]any{
				"op": 9,
				"d":  false,
			})
		case 4:
			// Server accepts session with READY -> ready becomes true, then requests reconnect.
			_ = conn.WriteJSON(map[string]any{
				"op": 0,
				"s":  1,
				"t":  "READY",
				"d":  map[string]any{"version": 1},
			})
			_ = conn.WriteJSON(map[string]any{
				"op": 7,
			})
		default:
			// Session 5: accept with RESUMED, then cancel context cleanly.
			_ = conn.WriteJSON(map[string]any{
				"op": 0,
				"s":  2,
				"t":  "RESUMED",
				"d":  map[string]any{},
			})
			cancel()
			<-ctx.Done()
		}
	})

	var mu sync.Mutex
	var waits []time.Duration

	pump := New(Config{
		Lookup:  staticLookup{url: wsURL},
		Tokens:  staticTokens{token: "tok"},
		Intents: 1 << 25,
		Logger:  slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})
	pump.wait = func(ctx context.Context, d time.Duration) error {
		mu.Lock()
		waits = append(waits, d)
		mu.Unlock()
		return nil
	}

	if err := pump.Run(ctx, func(context.Context, Envelope) error { return nil }); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	mu.Lock()
	got := append([]time.Duration(nil), waits...)
	mu.Unlock()

	want := []time.Duration{
		1 * time.Second, // after attempt 1 (op: 9 before READY)
		2 * time.Second, // after attempt 2 (op: 9 before READY)
		4 * time.Second, // after attempt 3 (op: 9 before READY)
		1 * time.Second, // after attempt 4 (READY received before op: 7 -> reset!)
	}
	if len(got) != len(want) {
		t.Fatalf("waits = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("waits[%d] = %v, want %v (all = %v)", i, got[i], want[i], got)
		}
	}
}

func TestPumpInitialHeartbeatNullSequenceThenAtomicLatestSequence(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nullHBCh := make(chan string, 1)
	zeroHBCh := make(chan string, 1)
	seq42HBCh := make(chan string, 1)

	_, wsURL := newWSServer(t, func(conn *websocket.Conn) {
		defer conn.Close()

		if err := conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 15},
		}); err != nil {
			return
		}

		phase := 0
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var pkt frame
			if err := json.Unmarshal(raw, &pkt); err != nil {
				continue
			}
			if pkt.Op != 1 {
				continue
			}
			_ = conn.WriteJSON(map[string]any{"op": 11})

			dText := strings.TrimSpace(string(pkt.D))
			switch phase {
			case 0:
				nullHBCh <- dText
				phase = 1
				// Send sequence 0 explicitly to verify 0 is distinguished from nil!
				_ = conn.WriteJSON(map[string]any{
					"op": 0,
					"s":  0,
					"t":  "READY",
					"d":  map[string]any{},
				})
			case 1:
				if dText == "0" {
					zeroHBCh <- dText
					phase = 2
					_ = conn.WriteJSON(map[string]any{
						"op": 0,
						"s":  42,
						"t":  "C2C_MESSAGE_CREATE",
						"d": map[string]any{
							"id":        "m-42",
							"content":   "hi",
							"timestamp": "2026-03-29T15:00:00Z",
							"author":    map[string]any{"user_openid": "u-1"},
						},
					})
				}
			case 2:
				if dText == "42" {
					seq42HBCh <- dText
					cancel()
					return
				}
			}
		}
	})

	pump := New(Config{
		Lookup: staticLookup{url: wsURL},
		Tokens: staticTokens{token: "tok"},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})

	done := make(chan error, 1)
	go func() {
		done <- pump.Run(ctx, func(context.Context, Envelope) error { return nil })
	}()

	select {
	case got := <-nullHBCh:
		if got != "null" {
			t.Fatalf("initial heartbeat d = %s, want null", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial null heartbeat")
	}

	select {
	case got := <-zeroHBCh:
		if got != "0" {
			t.Fatalf("post-seq-0 heartbeat d = %s, want 0", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for seq=0 heartbeat")
	}

	select {
	case got := <-seq42HBCh:
		if got != "42" {
			t.Fatalf("post-seq-42 heartbeat d = %s, want 42", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for seq=42 heartbeat")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run() exit")
	}
}

func TestPumpConcurrentIdentifyAndHeartbeatWriteSafetyAndSingleHeartbeat(t *testing.T) {
	t.Parallel()

	t.Run("concurrent_writes_under_race", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var heartbeats atomic.Int32
		var identifies atomic.Int32

		_, wsURL := newWSServer(t, func(conn *websocket.Conn) {
			defer conn.Close()

			// Read loop in background so client writes never stall TCP buffers.
			go func() {
				for {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					var pkt frame
					if json.Unmarshal(raw, &pkt) == nil {
						switch pkt.Op {
						case 1:
							heartbeats.Add(1)
						case 2:
							identifies.Add(1)
						}
					}
				}
			}()

			// Send multiple op: 10 Hello frames with 1ms heartbeat_interval to force
			// concurrent identify + heartbeat + close frame writes under -race.
			for i := 0; i < 5; i++ {
				if err := conn.WriteJSON(map[string]any{
					"op": 10,
					"d":  map[string]any{"heartbeat_interval": 1},
				}); err != nil {
					return
				}
				_ = conn.WriteJSON(map[string]any{
					"op": 11,
				})
				time.Sleep(3 * time.Millisecond)
			}

			for heartbeats.Load() < 5 {
				_ = conn.WriteJSON(map[string]any{"op": 11})
				time.Sleep(2 * time.Millisecond)
			}
			cancel()
			<-ctx.Done()
		})

		pump := New(Config{
			Lookup: staticLookup{url: wsURL},
			Tokens: staticTokens{token: "tok"},
			Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		})

		if err := pump.Run(ctx, func(context.Context, Envelope) error { return nil }); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		if identifies.Load() < 1 {
			t.Fatalf("identifies = %d, want >= 1", identifies.Load())
		}
		if heartbeats.Load() < 5 {
			t.Fatalf("heartbeats = %d, want >= 5", heartbeats.Load())
		}
	})

	t.Run("duplicate_hello_spawns_only_one_heartbeat_goroutine", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var heartbeats atomic.Int32
		doneWindow := make(chan struct{})

		_, wsURL := newWSServer(t, func(conn *websocket.Conn) {
			defer conn.Close()

			go func() {
				for {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						return
					}
					var pkt frame
					if json.Unmarshal(raw, &pkt) == nil && pkt.Op == 1 {
						heartbeats.Add(1)
					}
				}
			}()

			// Send 4 duplicate Hello frames on the same session with 30ms interval.
			// Over 75ms, 1 goroutine produces ~2 heartbeats; 4 goroutines would produce ~8+.
			for i := 0; i < 4; i++ {
				if err := conn.WriteJSON(map[string]any{
					"op": 10,
					"d":  map[string]any{"heartbeat_interval": 30},
				}); err != nil {
					return
				}
			}

			time.Sleep(75 * time.Millisecond)
			close(doneWindow)
			cancel()
			<-ctx.Done()
		})

		pump := New(Config{
			Lookup: staticLookup{url: wsURL},
			Tokens: staticTokens{token: "tok"},
			Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		})

		if err := pump.Run(ctx, func(context.Context, Envelope) error { return nil }); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		<-doneWindow

		got := heartbeats.Load()
		if got < 1 || got > 4 {
			t.Fatalf("heartbeats over 75ms (30ms interval, 4 Hellos) = %d, want between 1 and 4 (single goroutine)", got)
		}
	})
}

func TestPumpConsumeErrorDoesNotDropSession(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sessions atomic.Int32
	_, wsURL := newWSServer(t, func(conn *websocket.Conn) {
		defer conn.Close()
		sessions.Add(1)

		_ = conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 1000},
		})
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{
			"op": 0,
			"s":  1,
			"t":  "READY",
			"d":  map[string]any{},
		})
		_ = conn.WriteJSON(map[string]any{
			"op": 0,
			"s":  2,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "msg-fail",
				"content":   "first",
				"timestamp": "2026-03-29T15:00:00Z",
				"author":    map[string]any{"user_openid": "user-1"},
			},
		})
		_ = conn.WriteJSON(map[string]any{
			"op": 0,
			"s":  3,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "msg-ok",
				"content":   "second",
				"timestamp": "2026-03-29T15:00:01Z",
				"author":    map[string]any{"user_openid": "user-1"},
			},
		})
		<-ctx.Done()
	})

	var logBuf bytes.Buffer
	var logMu sync.Mutex
	safeWriter := writerFunc(func(p []byte) (int, error) {
		logMu.Lock()
		defer logMu.Unlock()
		return logBuf.Write(p)
	})

	pump := New(Config{
		Lookup: staticLookup{url: wsURL},
		Tokens: staticTokens{token: "tok"},
		Logger: slog.New(slog.NewTextHandler(safeWriter, nil)),
	})

	var received []string
	err := pump.Run(ctx, func(_ context.Context, env Envelope) error {
		received = append(received, env.ID)
		if env.ID == "msg-fail" {
			return errors.New("downstream reply failed")
		}
		cancel()
		return nil
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if got := sessions.Load(); got != 1 {
		t.Fatalf("websocket sessions = %d, want 1 (session must stay alive after consume error)", got)
	}
	if len(received) != 2 || received[0] != "msg-fail" || received[1] != "msg-ok" {
		t.Fatalf("received events = %v, want [msg-fail msg-ok]", received)
	}
	logMu.Lock()
	logs := logBuf.String()
	logMu.Unlock()
	if !strings.Contains(logs, "downstream reply failed") {
		t.Fatalf("expected error logged, got logs = %q", logs)
	}
}

func TestPumpDeadSocketReadDeadlineAndHeartbeatAckLiveness(t *testing.T) {
	t.Parallel()

	if got := defaultReadTimeout(30 * time.Second); got != 75*time.Second {
		t.Fatalf("defaultReadTimeout(30s) = %v, want 75s", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sessions atomic.Int32
	reconnected := make(chan Envelope, 1)

	_, wsURL := newWSServer(t, func(conn *websocket.Conn) {
		defer conn.Close()
		sessionNum := sessions.Add(1)
		var srvWriteMu sync.Mutex
		writeServerJSON := func(v any) error {
			srvWriteMu.Lock()
			defer srvWriteMu.Unlock()
			return conn.WriteJSON(v)
		}

		// Drain incoming client frames so writes don't fail first; we specifically
		// test read-deadline liveness and half-open silent peer detection.
		stopAck := make(chan struct{})
		go func() {
			for {
				_, raw, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var pkt frame
				if json.Unmarshal(raw, &pkt) == nil && pkt.Op == 1 {
					select {
					case <-stopAck:
					default:
						_ = writeServerJSON(map[string]any{"op": 11})
					}
				}
			}
		}()

		if err := writeServerJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 15},
		}); err != nil {
			return
		}
		_ = writeServerJSON(map[string]any{
			"op": 0,
			"s":  1,
			"t":  "READY",
			"d":  map[string]any{},
		})

		if sessionNum == 1 {
			// Keep sending op: 11 Heartbeat ACKs for 140ms (> 2x 60ms readWindow)
			// to prove incoming op: 11 frames refresh SetReadDeadline!
			time.Sleep(140 * time.Millisecond)
			if sessions.Load() != 1 {
				return
			}
			// Now simulate a half-open dead socket: stop sending all frames while keeping TCP open.
			close(stopAck)
			<-ctx.Done()
			return
		}

		// Session 2: deliver an event proving clean recovery after dead-socket timeout.
		_ = writeServerJSON(map[string]any{
			"op": 0,
			"s":  2,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "after-dead-socket",
				"content":   "recovered",
				"timestamp": "2026-03-29T15:00:00Z",
				"author":    map[string]any{"user_openid": "user-1"},
			},
		})
		<-ctx.Done()
	})

	pump := New(Config{
		Lookup: staticLookup{url: wsURL},
		Tokens: staticTokens{token: "tok"},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})
	// Configure a short read timeout (2*15ms + 30ms = 60ms) so the test completes in ~200ms.
	pump.readTimeout = func(hb time.Duration) time.Duration {
		if hb <= 0 {
			return 200 * time.Millisecond
		}
		return 2*hb + 30*time.Millisecond
	}
	pump.wait = func(context.Context, time.Duration) error {
		return nil
	}

	done := make(chan error, 1)
	go func() {
		done <- pump.Run(ctx, func(_ context.Context, env Envelope) error {
			reconnected <- env
			cancel()
			return nil
		})
	}()

	select {
	case env := <-reconnected:
		if env.ID != "after-dead-socket" {
			t.Fatalf("env.ID = %q, want %q", env.ID, "after-dead-socket")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for dead-socket detection and reconnect")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run() exit")
	}

	if got := sessions.Load(); got != 2 {
		t.Fatalf("sessions = %d, want 2 (1 survived via ACK then timed out when silent, 2 recovered)", got)
	}
}

type failOnHeartbeatConn struct {
	net.Conn
	failWrites atomic.Bool
}

func (f *failOnHeartbeatConn) Write(p []byte) (int, error) {
	if f.failWrites.Load() {
		return 0, errors.New("simulated broken pipe on heartbeat write")
	}
	return f.Conn.Write(p)
}

func TestPumpHeartbeatWriteErrorTriggersReconnect(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sessions atomic.Int32
	var wrappedFirst *failOnHeartbeatConn
	var wrappedMu sync.Mutex

	_, wsURL := newWSServer(t, func(conn *websocket.Conn) {
		defer conn.Close()
		sessionNum := sessions.Add(1)

		_ = conn.WriteJSON(map[string]any{
			"op": 10,
			"d":  map[string]any{"heartbeat_interval": 15},
		})
		// Read IDENTIFY (op: 2)
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}

		if sessionNum == 1 {
			wrappedMu.Lock()
			first := wrappedFirst
			wrappedMu.Unlock()
			if first != nil {
				first.failWrites.Store(true)
			}
			// Stay silent with a huge readTimeout (30s) so ONLY heartbeat WriteJSON
			// error can trigger session close and reconnect rapidly.
			<-ctx.Done()
			return
		}

		_ = conn.WriteJSON(map[string]any{
			"op": 0,
			"s":  1,
			"t":  "C2C_MESSAGE_CREATE",
			"d": map[string]any{
				"id":        "recovered-from-write-err",
				"content":   "ok",
				"timestamp": "2026-03-29T15:00:00Z",
				"author":    map[string]any{"user_openid": "user-1"},
			},
		})
		<-ctx.Done()
	})

	pump := New(Config{
		Lookup: staticLookup{url: wsURL},
		Tokens: staticTokens{token: "tok"},
		Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	})
	var dialNum atomic.Int32
	baseDialer := &net.Dialer{}
	pump.dialer = &websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := baseDialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if dialNum.Add(1) == 1 {
				wrapped := &failOnHeartbeatConn{Conn: c}
				wrappedMu.Lock()
				wrappedFirst = wrapped
				wrappedMu.Unlock()
				return wrapped, nil
			}
			return c, nil
		},
	}
	pump.readTimeout = func(time.Duration) time.Duration {
		return 30 * time.Second
	}
	pump.wait = func(context.Context, time.Duration) error {
		return nil
	}

	gotCh := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- pump.Run(ctx, func(_ context.Context, env Envelope) error {
			gotCh <- env.ID
			cancel()
			return nil
		})
	}()

	select {
	case id := <-gotCh:
		if id != "recovered-from-write-err" {
			t.Fatalf("event id = %q, want %q", id, "recovered-from-write-err")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for heartbeat write error reconnect")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Run() exit")
	}

	if got := sessions.Load(); got != 2 {
		t.Fatalf("sessions = %d, want 2", got)
	}
}

type writerFunc func([]byte) (int, error)

func (fn writerFunc) Write(p []byte) (int, error) {
	return fn(p)
}
