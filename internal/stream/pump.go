package stream

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type GatewayLookup interface {
	Gateway(context.Context) (string, error)
}

type TokenProvider interface {
	Token(context.Context) (string, error)
}

type Config struct {
	Lookup  GatewayLookup
	Tokens  TokenProvider
	Intents int
	Logger  *slog.Logger
}

type Pump struct {
	lookup      GatewayLookup
	tokens      TokenProvider
	intents     int
	logger      *slog.Logger
	dialer      *websocket.Dialer
	wait        func(context.Context, time.Duration) error
	readTimeout func(time.Duration) time.Duration
}

type sessionConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (s *sessionConn) writeJSON(v any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.conn.WriteJSON(v)
}

func (s *sessionConn) bestEffortCloseFrame() {
	if !s.writeMu.TryLock() {
		return
	}
	defer s.writeMu.Unlock()
	msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
	_ = s.conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(100*time.Millisecond))
}

func (s *sessionConn) close() error {
	return s.conn.Close()
}

type Envelope struct {
	ID   string
	Kind string
	From string
	To   Destination
	Text string
	At   time.Time
	Raw  json.RawMessage
}

type Destination struct {
	Kind string
	ID   string
}

type frame struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
	S  *int64          `json:"s,omitempty"`
	T  string          `json:"t,omitempty"`
}

type hello struct {
	HeartbeatMS int64 `json:"heartbeat_interval"`
}

type directEvent struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
	Author    struct {
		User string `json:"user_openid"`
	} `json:"author"`
}

type groupEvent struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
	GroupID   string    `json:"group_openid"`
	Author    struct {
		Member string `json:"member_openid"`
	} `json:"author"`
}

func New(cfg Config) *Pump {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	intents := cfg.Intents
	if intents <= 0 {
		intents = 1 << 25
	}
	return &Pump{
		lookup:      cfg.Lookup,
		tokens:      cfg.Tokens,
		intents:     intents,
		logger:      logger,
		dialer:      websocket.DefaultDialer,
		wait:        pause,
		readTimeout: defaultReadTimeout,
	}
}

func (p *Pump) effectiveDialer() *websocket.Dialer {
	if p.dialer != nil {
		return p.dialer
	}
	return websocket.DefaultDialer
}

func (p *Pump) effectiveWait(ctx context.Context, d time.Duration) error {
	if p.wait != nil {
		return p.wait(ctx, d)
	}
	return pause(ctx, d)
}

func (p *Pump) effectiveReadTimeout(heartbeatInterval time.Duration) time.Duration {
	if p.readTimeout != nil {
		return p.readTimeout(heartbeatInterval)
	}
	return defaultReadTimeout(heartbeatInterval)
}

func defaultReadTimeout(heartbeatInterval time.Duration) time.Duration {
	if heartbeatInterval <= 0 {
		return 45 * time.Second
	}
	return 2*heartbeatInterval + 15*time.Second
}

func (p *Pump) Run(ctx context.Context, consume func(context.Context, Envelope) error) error {
	if consume == nil {
		return errors.New("consume 不能为空")
	}

	delay := time.Second
	for {
		ready, err := p.runSession(ctx, consume)
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return nil
		}
		if err.Error() != "gateway requested reconnect" && p.logger != nil {
			p.logger.Warn("gateway stopped", "err", err)
		}

		var current time.Duration
		if ready {
			delay = time.Second
			current = delay
		} else {
			current = delay
			if delay < 30*time.Second {
				delay *= 2
				if delay > 30*time.Second {
					delay = 30 * time.Second
				}
			}
		}

		if err := p.effectiveWait(ctx, current); err != nil {
			return nil
		}
	}
}

func (p *Pump) runSession(ctx context.Context, consume func(context.Context, Envelope) error) (bool, error) {
	wsURL, err := p.lookup.Gateway(ctx)
	if err != nil {
		return false, err
	}
	token, err := p.tokens.Token(ctx)
	if err != nil {
		return false, err
	}

	rawConn, _, err := p.effectiveDialer().DialContext(ctx, wsURL, nil)
	if err != nil {
		return false, err
	}
	sc := &sessionConn{conn: rawConn}

	sessionDone := make(chan struct{})
	var bgWG sync.WaitGroup
	defer func() {
		close(sessionDone)
		_ = sc.close()
		bgWG.Wait()
	}()

	bgWG.Add(1)
	go func() {
		defer bgWG.Done()
		select {
		case <-ctx.Done():
			sc.bestEffortCloseFrame()
			_ = sc.close()
		case <-sessionDone:
		}
	}()

	var ready bool
	var seq atomic.Pointer[int64]
	var heartOnce sync.Once

	readWindow := p.effectiveReadTimeout(0)
	if readWindow > 0 {
		_ = sc.conn.SetReadDeadline(time.Now().Add(readWindow))
	}

	for {
		select {
		case <-ctx.Done():
			return ready, nil
		default:
		}

		_, raw, err := sc.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ready, nil
			}
			return ready, err
		}
		if readWindow > 0 {
			_ = sc.conn.SetReadDeadline(time.Now().Add(readWindow))
		}

		packet := frame{}
		if err := json.Unmarshal(raw, &packet); err != nil {
			continue
		}
		if packet.S != nil {
			s := *packet.S
			seq.Store(&s)
		}

		switch packet.Op {
		case 10:
			greeting := hello{}
			if err := json.Unmarshal(packet.D, &greeting); err != nil {
				return ready, err
			}
			if greeting.HeartbeatMS > 0 {
				hbInterval := time.Duration(greeting.HeartbeatMS) * time.Millisecond
				readWindow = p.effectiveReadTimeout(hbInterval)
				if readWindow > 0 {
					_ = sc.conn.SetReadDeadline(time.Now().Add(readWindow))
				}
				heartOnce.Do(func() {
					bgWG.Add(1)
					go func() {
						defer bgWG.Done()
						heartbeat(ctx, sc, hbInterval, &seq, sessionDone)
					}()
				})
			}
			if err := identify(sc, token, p.intents); err != nil {
				return ready, err
			}
		case 11:
			continue
		case 7:
			return ready, errors.New("gateway requested reconnect")
		case 9:
			return ready, errors.New("gateway session invalid")
		case 0:
			eventType := strings.TrimSpace(packet.T)
			if eventType == "READY" || eventType == "RESUMED" {
				ready = true
				continue
			}
			envelope, ok := decode(eventType, packet.D)
			if !ok {
				continue
			}
			if err := consume(ctx, envelope); err != nil {
				if ctx.Err() != nil {
					return ready, nil
				}
				if p.logger != nil {
					p.logger.Warn("consume event failed", "type", eventType, "id", envelope.ID, "err", err)
				}
			}
			if readWindow > 0 {
				_ = sc.conn.SetReadDeadline(time.Now().Add(readWindow))
			}
		}
	}
}

func decode(kind string, raw json.RawMessage) (Envelope, bool) {
	switch strings.TrimSpace(kind) {
	case "C2C_MESSAGE_CREATE":
		event := directEvent{}
		if err := json.Unmarshal(raw, &event); err != nil {
			return Envelope{}, false
		}
		if strings.TrimSpace(event.Author.User) == "" {
			return Envelope{}, false
		}
		return Envelope{
			ID:   event.ID,
			Kind: "direct_message",
			From: event.Author.User,
			To: Destination{
				Kind: "direct",
				ID:   event.Author.User,
			},
			Text: event.Content,
			At:   event.Timestamp,
			Raw:  raw,
		}, true
	case "GROUP_AT_MESSAGE_CREATE":
		event := groupEvent{}
		if err := json.Unmarshal(raw, &event); err != nil {
			return Envelope{}, false
		}
		if strings.TrimSpace(event.GroupID) == "" {
			return Envelope{}, false
		}
		return Envelope{
			ID:   event.ID,
			Kind: "group_mention",
			From: event.Author.Member,
			To: Destination{
				Kind: "group",
				ID:   event.GroupID,
			},
			Text: event.Content,
			At:   event.Timestamp,
			Raw:  raw,
		}, true
	default:
		return Envelope{}, false
	}
}

func identify(sc *sessionConn, token string, intents int) error {
	return sc.writeJSON(map[string]any{
		"op": 2,
		"d": map[string]any{
			"token":   "QQBot " + token,
			"intents": intents,
			"shard":   []int{0, 1},
		},
	})
}

func heartbeat(ctx context.Context, sc *sessionConn, interval time.Duration, seq *atomic.Pointer[int64], stop <-chan struct{}) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			var d any
			if current := seq.Load(); current != nil {
				d = *current
			}
			if err := sc.writeJSON(map[string]any{
				"op": 1,
				"d":  d,
			}); err != nil {
				_ = sc.close()
				return
			}
		}
	}
}

func pause(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
