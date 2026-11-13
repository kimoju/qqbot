package stream

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultWorkers       = 8
	defaultQueueSize     = 128
	defaultReconnectBase = time.Second
	defaultReconnectMax  = 30 * time.Second
	defaultHealthyWindow = 30 * time.Second
)

var (
	errGatewayReconnect = errors.New("gateway requested reconnect")
	errInvalidSession   = errors.New("gateway session invalid")
)

type GatewayLookup interface {
	Gateway(context.Context) (string, error)
}

type TokenProvider interface {
	Token(context.Context) (string, error)
}

type InvalidatableTokenProvider interface {
	TokenProvider
	Invalidate()
}

func invalidateToken(p TokenProvider) {
	if inv, ok := p.(InvalidatableTokenProvider); ok {
		inv.Invalidate()
	}
}

type Config struct {
	Lookup    GatewayLookup
	Tokens    TokenProvider
	Intents   int
	Logger    *slog.Logger
	Workers   int
	QueueSize int
}

type Pump struct {
	lookup        GatewayLookup
	tokens        TokenProvider
	intents       int
	logger        *slog.Logger
	workers       int
	queueSize     int
	dialer        *websocket.Dialer
	wait          func(context.Context, time.Duration) error
	jitter        func(time.Duration) time.Duration
	now           func() time.Time
	healthyWindow time.Duration
	readTimeout   func(time.Duration) time.Duration

	stateMu        sync.Mutex
	sessionID      string
	lastSeq        int64
	hasSeq         bool
	resumeFailures int
}

type sessionConn struct {
	conn      *websocket.Conn
	writeMu   sync.Mutex
	controlMu sync.Mutex
}

func (s *sessionConn) writeJSON(v any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_ = s.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return s.conn.WriteJSON(v)
}

func (s *sessionConn) writeControl(messageType int, data []byte, timeout time.Duration) error {
	s.controlMu.Lock()
	defer s.controlMu.Unlock()
	return s.conn.WriteControl(messageType, data, time.Now().Add(timeout))
}

func (s *sessionConn) bestEffortCloseFrame() {
	if !s.controlMu.TryLock() {
		return
	}
	defer s.controlMu.Unlock()
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

type dispatchedEvent struct {
	eventType string
	envelope  Envelope
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

type readyPayload struct {
	SessionID string `json:"session_id"`
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

type sessionOutcome struct {
	ready              bool
	healthyDuration    time.Duration
	requestedReconnect bool
	invalidSession     bool
	invalidatedResume  bool
	err                error
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
	workers := cfg.Workers
	if workers <= 0 {
		workers = defaultWorkers
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	return &Pump{
		lookup:        cfg.Lookup,
		tokens:        cfg.Tokens,
		intents:       intents,
		logger:        logger,
		workers:       workers,
		queueSize:     queueSize,
		dialer:        websocket.DefaultDialer,
		wait:          pause,
		jitter:        defaultJitter,
		now:           time.Now,
		healthyWindow: defaultHealthyWindow,
		readTimeout:   defaultReadTimeout,
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

func (p *Pump) effectiveJitter(d time.Duration) time.Duration {
	if p.jitter != nil {
		return p.jitter(d)
	}
	return defaultJitter(d)
}

func (p *Pump) effectiveNow() time.Time {
	if p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *Pump) effectiveHealthyWindow() time.Duration {
	if p.healthyWindow > 0 {
		return p.healthyWindow
	}
	return defaultHealthyWindow
}

func (p *Pump) effectiveWorkers() int {
	if p.workers > 0 {
		return p.workers
	}
	return defaultWorkers
}

func (p *Pump) effectiveQueueSize() int {
	if p.queueSize > 0 {
		return p.queueSize
	}
	return defaultQueueSize
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

func defaultJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	// ±20% uniform jitter in [0.8*d, 1.2*d]
	factor := 0.8 + rand.Float64()*0.4
	out := time.Duration(float64(d) * factor)
	if out <= 0 {
		return 1
	}
	return out
}

func (p *Pump) getResumeState() (sessionID string, lastSeq int64, hasSeq bool) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	return p.sessionID, p.lastSeq, p.hasSeq
}

func (p *Pump) setSessionID(sessionID string) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.sessionID = sessionID
}

func (p *Pump) setLastSeq(seq int64) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.lastSeq = seq
	p.hasSeq = true
}

func (p *Pump) resetResumeFailures() {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.resumeFailures = 0
}

func (p *Pump) incResumeFailures() int {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.resumeFailures++
	return p.resumeFailures
}

func (p *Pump) clearSession() {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.sessionID = ""
	p.lastSeq = 0
	p.hasSeq = false
	p.resumeFailures = 0
}

func classifyCloseError(err error) (clearSession bool, invalidateTok bool) {
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) {
		return false, false
	}
	switch {
	case closeErr.Code == 4004 || closeErr.Code == 4914 || closeErr.Code == 4915:
		return true, true
	case closeErr.Code == 4006 || closeErr.Code == 4007 || closeErr.Code == 4009 ||
		(closeErr.Code >= 4010 && closeErr.Code <= 4014) ||
		(closeErr.Code >= 4900 && closeErr.Code <= 4913):
		return true, false
	default:
		return false, false
	}
}

func (p *Pump) Run(ctx context.Context, consume func(context.Context, Envelope) error) error {
	if consume == nil {
		return errors.New("consume 不能为空")
	}
	if ctx.Err() != nil {
		return nil
	}

	dispatchCh := make(chan dispatchedEvent, p.effectiveQueueSize())
	var workerWG sync.WaitGroup
	workerCount := p.effectiveWorkers()
	for i := 0; i < workerCount; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for item := range dispatchCh {
				if ctx.Err() != nil {
					continue
				}
				if err := consume(ctx, item.envelope); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						continue
					}
					if p.logger != nil {
						p.logger.Warn("consume event failed", "type", item.eventType, "id", item.envelope.ID, "err", err)
					}
				}
			}
		}()
	}
	defer func() {
		close(dispatchCh)
		workerWG.Wait()
		p.clearSession()
	}()

	delay := defaultReconnectBase
	for {
		if ctx.Err() != nil {
			return nil
		}

		outcome := p.runSession(ctx, dispatchCh)
		if ctx.Err() != nil {
			return nil
		}
		if outcome.err == nil {
			return nil
		}
		if !errors.Is(outcome.err, errGatewayReconnect) && p.logger != nil {
			p.logger.Warn("gateway stopped", "err", outcome.err)
		}

		var baseWait time.Duration
		switch {
		case outcome.invalidatedResume:
			baseWait = 2 * time.Second
			if delay < defaultReconnectMax {
				delay *= 2
				if delay > defaultReconnectMax {
					delay = defaultReconnectMax
				}
			}
		case outcome.ready && (outcome.requestedReconnect || outcome.healthyDuration >= p.effectiveHealthyWindow()):
			delay = defaultReconnectBase
			baseWait = delay
		default:
			baseWait = delay
			if delay < defaultReconnectMax {
				delay *= 2
				if delay > defaultReconnectMax {
					delay = defaultReconnectMax
				}
			}
		}

		waitDur := p.effectiveJitter(baseWait)
		if err := p.effectiveWait(ctx, waitDur); err != nil {
			return nil
		}
	}
}

func (p *Pump) runSession(ctx context.Context, dispatchCh chan<- dispatchedEvent) sessionOutcome {
	wsURL, err := p.lookup.Gateway(ctx)
	if err != nil {
		return sessionOutcome{err: err}
	}
	token, err := p.tokens.Token(ctx)
	if err != nil {
		return sessionOutcome{err: err}
	}

	rawConn, _, err := p.effectiveDialer().DialContext(ctx, wsURL, nil)
	if err != nil {
		return sessionOutcome{err: err}
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
	var readyAt time.Time
	var sentResume bool
	var seq atomic.Pointer[int64]
	if initSID, initSeq, initHasSeq := p.getResumeState(); initSID != "" && initHasSeq {
		s := initSeq
		seq.Store(&s)
	}
	var heartOnce sync.Once

	healthyDur := func() time.Duration {
		if !ready || readyAt.IsZero() {
			return 0
		}
		d := p.effectiveNow().Sub(readyAt)
		if d < 0 {
			return 0
		}
		return d
	}

	var readWindowNS atomic.Int64
	readWindowNS.Store(int64(p.effectiveReadTimeout(0)))
	refreshReadDeadline := func() {
		if rw := time.Duration(readWindowNS.Load()); rw > 0 {
			_ = sc.conn.SetReadDeadline(time.Now().Add(rw))
		}
	}
	refreshReadDeadline()

	sc.conn.SetPingHandler(func(appData string) error {
		refreshReadDeadline()
		return sc.writeControl(websocket.PongMessage, []byte(appData), 5*time.Second)
	})
	sc.conn.SetPongHandler(func(string) error {
		refreshReadDeadline()
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return sessionOutcome{ready: ready, healthyDuration: healthyDur()}
		default:
		}

		_, raw, err := sc.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return sessionOutcome{ready: ready, healthyDuration: healthyDur()}
			}
			clearSess, invTok := classifyCloseError(err)
			if clearSess {
				p.clearSession()
			}
			if invTok {
				invalidateToken(p.tokens)
			}
			return sessionOutcome{
				ready:           ready,
				healthyDuration: healthyDur(),
				err:             err,
			}
		}
		refreshReadDeadline()

		packet := frame{}
		if err := json.Unmarshal(raw, &packet); err != nil {
			continue
		}
		if packet.S != nil {
			s := *packet.S
			seq.Store(&s)
			p.setLastSeq(s)
		}

		switch packet.Op {
		case 10:
			greeting := hello{}
			if err := json.Unmarshal(packet.D, &greeting); err != nil {
				return sessionOutcome{ready: ready, healthyDuration: healthyDur(), err: err}
			}
			if greeting.HeartbeatMS > 0 {
				hbInterval := time.Duration(greeting.HeartbeatMS) * time.Millisecond
				readWindowNS.Store(int64(p.effectiveReadTimeout(hbInterval)))
				refreshReadDeadline()
				heartOnce.Do(func() {
					bgWG.Add(1)
					go func() {
						defer bgWG.Done()
						heartbeat(ctx, sc, hbInterval, &seq, sessionDone)
					}()
				})
			}
			resumeSID, resumeSeq, _ := p.getResumeState()
			if resumeSID != "" {
				sentResume = true
				if err := resume(sc, token, resumeSID, resumeSeq); err != nil {
					return sessionOutcome{ready: ready, healthyDuration: healthyDur(), err: err}
				}
			} else {
				sentResume = false
				if err := identify(sc, token, p.intents); err != nil {
					return sessionOutcome{ready: ready, healthyDuration: healthyDur(), err: err}
				}
			}
		case 11:
			continue
		case 7:
			return sessionOutcome{
				ready:              ready,
				healthyDuration:    healthyDur(),
				requestedReconnect: true,
				err:                errGatewayReconnect,
			}
		case 9:
			var resumable bool
			_ = json.Unmarshal(packet.D, &resumable)
			if !sentResume {
				p.clearSession()
				invalidateToken(p.tokens)
			} else if !resumable || p.incResumeFailures() > 1 {
				p.clearSession()
			}
			return sessionOutcome{
				ready:             ready,
				healthyDuration:   healthyDur(),
				invalidSession:    true,
				invalidatedResume: sentResume,
				err:               errInvalidSession,
			}
		case 0:
			eventType := strings.TrimSpace(packet.T)
			if eventType == "READY" {
				var info readyPayload
				if err := json.Unmarshal(packet.D, &info); err == nil {
					if sid := strings.TrimSpace(info.SessionID); sid != "" {
						p.setSessionID(sid)
					}
				}
				p.resetResumeFailures()
				if !ready {
					ready = true
					readyAt = p.effectiveNow()
				}
				continue
			}
			if eventType == "RESUMED" {
				p.resetResumeFailures()
				if !ready {
					ready = true
					readyAt = p.effectiveNow()
				}
				continue
			}
			envelope, ok := decode(eventType, packet.D)
			if !ok {
				continue
			}
			select {
			case <-ctx.Done():
				return sessionOutcome{ready: ready, healthyDuration: healthyDur()}
			case dispatchCh <- dispatchedEvent{eventType: eventType, envelope: envelope}:
			default:
				if p.logger != nil {
					p.logger.Warn("dispatch queue full, dropping event", "type", eventType, "id", envelope.ID)
				}
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

func resume(sc *sessionConn, token string, sessionID string, seq int64) error {
	return sc.writeJSON(map[string]any{
		"op": 6,
		"d": map[string]any{
			"token":      "QQBot " + token,
			"session_id": sessionID,
			"seq":        seq,
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
