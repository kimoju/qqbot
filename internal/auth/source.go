package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxRefreshLead       = 45 * time.Second
	defaultTokenLifetime = 7200
)

type Config struct {
	AppID     string
	AppSecret string
	Endpoint  string
	Client    *http.Client
}

type Source struct {
	client   *http.Client
	endpoint string
	appID    string
	secret   string
	now      func() time.Time

	mu        sync.RWMutex
	token     string
	expires   time.Time
	refreshAt time.Time
}

type tokenPayload struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   any    `json:"expires_in"`
}

func New(cfg Config) (*Source, error) {
	if strings.TrimSpace(cfg.AppID) == "" {
		return nil, errors.New("app_id 不能为空")
	}
	if strings.TrimSpace(cfg.AppSecret) == "" {
		return nil, errors.New("app_secret 不能为空")
	}
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, errors.New("endpoint 不能为空")
	}
	if cfg.Client == nil {
		return nil, errors.New("client 不能为空")
	}

	return &Source{
		client:   cfg.Client,
		endpoint: strings.TrimSpace(cfg.Endpoint),
		appID:    cfg.AppID,
		secret:   cfg.AppSecret,
	}, nil
}

func (s *Source) effectiveNow() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func refreshLead(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return 0
	}
	lead := ttl / 3
	if lead > maxRefreshLead {
		lead = maxRefreshLead
	}
	return lead
}

func (s *Source) Token(ctx context.Context) (string, error) {
	now := s.effectiveNow()
	s.mu.RLock()
	if s.token != "" && now.Before(s.refreshAt) {
		token := s.token
		s.mu.RUnlock()
		return token, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	now = s.effectiveNow()
	if s.token != "" && now.Before(s.refreshAt) {
		return s.token, nil
	}

	rawBody, _ := json.Marshal(map[string]string{
		"appId":        s.appID,
		"clientSecret": s.secret,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(rawBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("token request failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	payload := tokenPayload{}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", err
	}
	if strings.TrimSpace(payload.AccessToken) == "" {
		return "", errors.New("token 为空")
	}

	lifetime := seconds(payload.ExpiresIn)
	if lifetime <= 0 {
		lifetime = defaultTokenLifetime
	}
	fetchedAt := s.effectiveNow()
	ttl := time.Duration(lifetime) * time.Second
	lead := refreshLead(ttl)
	s.token = payload.AccessToken
	s.expires = fetchedAt.Add(ttl)
	s.refreshAt = fetchedAt.Add(ttl - lead)
	return s.token, nil
}

func (s *Source) Invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.token = ""
	s.expires = time.Time{}
	s.refreshAt = time.Time{}
}

func seconds(raw any) int64 {
	switch value := raw.(type) {
	case float64:
		return int64(value)
	case int64:
		return value
	case int:
		return int64(value)
	case json.Number:
		n, err := value.Int64()
		if err == nil {
			return n
		}
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err == nil {
			return n
		}
	}
	return 0
}
