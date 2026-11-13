package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maxResponseBytes = 1 << 20
	maxDrainBytes    = 64 << 10
	maxRetryShift    = 10
	maxRetryWait     = 30 * time.Second
)

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
	BaseURL     string
	Client      *http.Client
	Tokens      TokenProvider
	RetryCount  int
	RetryDelay  time.Duration
	DefaultKind string
}

type Client struct {
	baseURL     string
	client      *http.Client
	tokens      TokenProvider
	retryCount  int
	retryDelay  time.Duration
	defaultKind string
	now         func() time.Time
	sleep       func(context.Context, time.Duration) error
	jitter      func(time.Duration) time.Duration
}

type SendRequest struct {
	RecipientKind string
	RecipientID   string
	ContentKind   string
	Content       string
	Reply         *ReplyRequest
}

type ReplyRequest struct {
	MessageID string
	Sequence  int
	EventID   string
}

type SendResult struct {
	ID string
	At time.Time
}

type apiError struct {
	StatusCode int
	Code       int           `json:"code"`
	Message    string        `json:"message"`
	TraceID    string        `json:"-"`
	RetryAfter time.Duration `json:"-"`
}

func (e *apiError) Error() string {
	return fmt.Sprintf("qq api status=%d code=%d message=%s trace=%s", e.StatusCode, e.Code, e.Message, e.TraceID)
}

func (e *apiError) retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

type messageReply struct {
	ID        string `json:"id"`
	Timestamp any    `json:"timestamp"`
}

type gatewayReply struct {
	URL string `json:"url"`
}

func New(cfg Config) *Client {
	retryCount := cfg.RetryCount
	if retryCount < 0 {
		retryCount = 0
	}
	retryDelay := cfg.RetryDelay
	if retryDelay <= 0 {
		retryDelay = 200 * time.Millisecond
	}
	defaultKind := strings.TrimSpace(cfg.DefaultKind)
	if defaultKind == "" {
		defaultKind = "text"
	}

	client := cfg.Client
	if client == nil {
		client = http.DefaultClient
	}

	return &Client{
		baseURL:     strings.TrimRight(cfg.BaseURL, "/"),
		client:      client,
		tokens:      cfg.Tokens,
		retryCount:  retryCount,
		retryDelay:  retryDelay,
		defaultKind: defaultKind,
	}
}

func (c *Client) effectiveNow() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *Client) effectiveSleep(ctx context.Context, d time.Duration) error {
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) effectiveJitter(d time.Duration) time.Duration {
	if c.jitter != nil {
		return c.jitter(d)
	}
	return jitterDuration(d)
}

func jitterDuration(d time.Duration) time.Duration {
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

func (c *Client) retryWait(attempt int, apiErr *apiError) time.Duration {
	if apiErr != nil && apiErr.StatusCode == http.StatusTooManyRequests && apiErr.RetryAfter > 0 {
		if apiErr.RetryAfter > maxRetryWait {
			return maxRetryWait
		}
		return apiErr.RetryAfter
	}

	shift := attempt
	if shift < 0 {
		shift = 0
	} else if shift > maxRetryShift {
		shift = maxRetryShift
	}
	mult := time.Duration(1 << shift)

	delay := c.retryDelay
	if delay <= 0 {
		delay = 200 * time.Millisecond
	}
	if delay > maxRetryWait/mult {
		delay = maxRetryWait
	} else {
		delay *= mult
	}

	delay = c.effectiveJitter(delay)
	if delay > maxRetryWait {
		delay = maxRetryWait
	}
	if apiErr != nil && apiErr.RetryAfter > delay {
		if apiErr.RetryAfter > maxRetryWait {
			return maxRetryWait
		}
		return apiErr.RetryAfter
	}
	return delay
}

func (c *Client) Gateway(ctx context.Context) (string, error) {
	if c.tokens == nil {
		return "", errors.New("tokens 不能为空")
	}

	for authAttempt := 0; authAttempt < 2; authAttempt++ {
		token, err := c.tokens.Token(ctx)
		if err != nil {
			return "", err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/gateway", nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "QQBot "+token)

		resp, err := c.client.Do(req)
		if err != nil {
			return "", err
		}

		body, err := readAndCloseBody(resp.Body)
		if err != nil {
			return "", err
		}
		if resp.StatusCode == http.StatusUnauthorized && authAttempt == 0 {
			invalidateToken(c.tokens)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", parseAPIError(resp, body, c.effectiveNow())
		}

		reply := gatewayReply{}
		if err := json.Unmarshal(body, &reply); err != nil {
			return "", err
		}
		if strings.TrimSpace(reply.URL) == "" {
			return "", errors.New("gateway url 为空")
		}
		return reply.URL, nil
	}

	return "", errors.New("gateway request failed")
}

func (c *Client) Send(ctx context.Context, req SendRequest) (SendResult, error) {
	var lastErr error
	for attempt := 0; attempt <= c.retryCount; attempt++ {
		out, err := c.sendOnce(ctx, req)
		if err == nil {
			return out, nil
		}
		lastErr = err

		var apiErr *apiError
		if !errors.As(err, &apiErr) || !apiErr.retryable() || attempt == c.retryCount {
			return SendResult{}, err
		}

		wait := c.retryWait(attempt, apiErr)
		if err := c.effectiveSleep(ctx, wait); err != nil {
			return SendResult{}, err
		}
	}
	return SendResult{}, lastErr
}

func (c *Client) sendOnce(ctx context.Context, req SendRequest) (SendResult, error) {
	if c.tokens == nil {
		return SendResult{}, errors.New("tokens 不能为空")
	}

	endpoint, body, err := c.encode(req)
	if err != nil {
		return SendResult{}, err
	}

	for authAttempt := 0; authAttempt < 2; authAttempt++ {
		token, err := c.tokens.Token(ctx)
		if err != nil {
			return SendResult{}, err
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return SendResult{}, err
		}
		httpReq.Header.Set("Authorization", "QQBot "+token)
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := c.client.Do(httpReq)
		if err != nil {
			return SendResult{}, err
		}

		raw, err := readAndCloseBody(resp.Body)
		if err != nil {
			return SendResult{}, err
		}
		if resp.StatusCode == http.StatusUnauthorized && authAttempt == 0 {
			invalidateToken(c.tokens)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return SendResult{}, parseAPIError(resp, raw, c.effectiveNow())
		}

		reply := messageReply{}
		if err := json.Unmarshal(raw, &reply); err != nil {
			return SendResult{}, err
		}
		return SendResult{
			ID: reply.ID,
			At: parseMoment(reply.Timestamp),
		}, nil
	}

	return SendResult{}, errors.New("send request failed")
}

func readAndCloseBody(body io.ReadCloser) ([]byte, error) {
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDrainBytes))
		_ = body.Close()
	}()
	return io.ReadAll(io.LimitReader(body, maxResponseBytes))
}

func parseAPIError(resp *http.Response, raw []byte, now time.Time) *apiError {
	apiErr := &apiError{
		StatusCode: resp.StatusCode,
		TraceID:    resp.Header.Get("x-tps-trace-id"),
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), now),
	}
	if err := json.Unmarshal(raw, apiErr); err != nil && apiErr.Message == "" {
		apiErr.Message = strings.TrimSpace(string(raw))
	}
	return apiErr
}

func parseRetryAfter(raw string, now time.Time) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		maxSecs := int64(maxRetryWait / time.Second)
		if secs > maxSecs {
			return maxRetryWait
		}
		return time.Duration(secs) * time.Second
	}
	if secsFloat, err := strconv.ParseFloat(raw, 64); err == nil {
		if secsFloat <= 0 || math.IsNaN(secsFloat) || math.IsInf(secsFloat, 0) {
			return 0
		}
		if secsFloat >= maxRetryWait.Seconds() {
			return maxRetryWait
		}
		return time.Duration(secsFloat * float64(time.Second))
	}
	if ts, err := http.ParseTime(raw); err == nil {
		d := ts.Sub(now)
		if d <= 0 {
			return 0
		}
		if d > maxRetryWait {
			return maxRetryWait
		}
		return d
	}
	return 0
}

func escapePathSegment(seg string) string {
	escaped := url.PathEscape(seg)
	switch escaped {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return escaped
	}
}

func (c *Client) encode(req SendRequest) (string, []byte, error) {
	if strings.TrimSpace(req.RecipientID) == "" {
		return "", nil, errors.New("recipient_id 不能为空")
	}
	if strings.TrimSpace(req.Content) == "" {
		return "", nil, errors.New("content 不能为空")
	}

	contentKind := strings.TrimSpace(req.ContentKind)
	if contentKind == "" {
		contentKind = c.defaultKind
	}

	payload := map[string]any{}
	switch contentKind {
	case "text":
		payload["content"] = req.Content
		payload["msg_type"] = 0
	case "markdown":
		payload["markdown"] = map[string]any{"content": req.Content}
		payload["msg_type"] = 2
	default:
		return "", nil, errors.New("content_kind 只支持 text 或 markdown")
	}

	if req.Reply != nil {
		if strings.TrimSpace(req.Reply.MessageID) != "" {
			payload["msg_id"] = req.Reply.MessageID
		}
		if req.Reply.Sequence > 0 {
			payload["msg_seq"] = req.Reply.Sequence
		}
		if strings.TrimSpace(req.Reply.EventID) != "" {
			payload["event_id"] = req.Reply.EventID
		}
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", nil, err
	}

	escapedID := escapePathSegment(req.RecipientID)
	switch req.RecipientKind {
	case "c2c":
		return fmt.Sprintf("%s/v2/users/%s/messages", c.baseURL, escapedID), body, nil
	case "group":
		return fmt.Sprintf("%s/v2/groups/%s/messages", c.baseURL, escapedID), body, nil
	case "channel":
		return fmt.Sprintf("%s/channels/%s/messages", c.baseURL, escapedID), body, nil
	default:
		return "", nil, errors.New("recipient_kind 只支持 c2c/group/channel")
	}
}

func parseMoment(raw any) time.Time {
	switch value := raw.(type) {
	case string:
		value = strings.TrimSpace(value)
		if value == "" {
			return time.Now()
		}
		if ts, err := time.Parse(time.RFC3339, value); err == nil {
			return ts
		}
		if n, err := strconv.ParseInt(value, 10, 64); err == nil {
			return guessTime(n)
		}
	case float64:
		return guessTime(int64(value))
	case int64:
		return guessTime(value)
	case int:
		return guessTime(int64(value))
	case json.Number:
		if n, err := value.Int64(); err == nil {
			return guessTime(n)
		}
	}
	return time.Now()
}

func guessTime(raw int64) time.Time {
	if raw <= 0 {
		return time.Now()
	}
	if raw > 1_000_000_000_000 {
		return time.UnixMilli(raw)
	}
	return time.Unix(raw, 0)
}
