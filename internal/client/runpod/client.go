// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package runpod

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultBaseURL = "https://rest.runpod.io/v1"
	maxBodyBytes   = 1 << 20
	maxAttempts    = 4
)

var (
	ErrNotFound        = errors.New("runpod resource not found")
	ErrAmbiguous       = errors.New("multiple RunPod resources matched recovery lookup")
	ErrCreateAmbiguous = errors.New("RunPod create result is ambiguous")
)

type APIError struct {
	StatusCode int
	RequestID  string
}

func (e *APIError) Error() string {
	if e.RequestID == "" {
		return fmt.Sprintf("RunPod API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("RunPod API returned HTTP %d (request %s)", e.StatusCode, e.RequestID)
}

type Option func(*Client) error

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	apiKey     string
	sleep      func(context.Context, time.Duration) error
}

func New(rawKey []byte, opts ...Option) (*Client, error) {
	key := strings.TrimSpace(string(rawKey))
	if key == "" || len(key) > 4096 || strings.ContainsAny(key, "\r\n") {
		return nil, errors.New("RunPod API key is empty or malformed")
	}
	u, err := url.Parse(DefaultBaseURL)
	if err != nil {
		return nil, errors.New("invalid built-in RunPod API URL")
	}
	c := &Client{
		baseURL: u,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				TLSHandshakeTimeout:   5 * time.Second,
				ResponseHeaderTimeout: 20 * time.Second,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		apiKey: key,
		sleep: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
	}
	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithHTTPClient replaces the HTTP client, primarily for deterministic tests.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) error {
		if h == nil {
			return errors.New("HTTP client must not be nil")
		}
		c.httpClient = h
		return nil
	}
}

// WithBaseURLForTesting permits only loopback endpoints. Production callers
// cannot redirect the management credential to an arbitrary host.
func WithBaseURLForTesting(raw string) Option {
	return func(c *Client) error {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return errors.New("invalid test base URL")
		}
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("test base URL must be loopback")
		}
		c.baseURL = u
		return nil
	}
}

func (c *Client) GetTemplate(ctx context.Context, id string) (*Template, error) {
	var out Template
	if err := c.doJSON(ctx, http.MethodGet, "/templates/"+safeID(id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListTemplates(ctx context.Context) ([]Template, error) {
	var out []Template
	if err := c.doJSON(ctx, http.MethodGet, "/templates?includeEndpointBoundTemplates=true", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) FindTemplateByName(ctx context.Context, name string) (*Template, error) {
	all, err := c.ListTemplates(ctx)
	if err != nil {
		return nil, err
	}
	var found *Template
	for i := range all {
		if all[i].Name != name {
			continue
		}
		if found != nil {
			return nil, ErrAmbiguous
		}
		found = &all[i]
	}
	if found == nil {
		return nil, ErrNotFound
	}
	return found, nil
}

func (c *Client) CreateTemplate(ctx context.Context, in TemplateInput) (*Template, error) {
	var out Template
	if err := c.doJSON(ctx, http.MethodPost, "/templates", in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) UpdateTemplate(ctx context.Context, id string, in TemplateInput) (*Template, error) {
	// isServerless is create-only in the published update contract.
	in.IsServerless = false
	var out Template
	if err := c.doJSON(ctx, http.MethodPatch, "/templates/"+safeID(id), in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) DeleteTemplate(ctx context.Context, id string) error {
	return c.doJSON(ctx, http.MethodDelete, "/templates/"+safeID(id), nil, nil)
}

func (c *Client) GetEndpoint(ctx context.Context, id string) (*Endpoint, error) {
	var out Endpoint
	if err := c.doJSON(ctx, http.MethodGet, "/endpoints/"+safeID(id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	var out []Endpoint
	if err := c.doJSON(ctx, http.MethodGet, "/endpoints", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) FindEndpointByName(ctx context.Context, name string) (*Endpoint, error) {
	all, err := c.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	var found *Endpoint
	for i := range all {
		if all[i].Name != name {
			continue
		}
		if found != nil {
			return nil, ErrAmbiguous
		}
		found = &all[i]
	}
	if found == nil {
		return nil, ErrNotFound
	}
	return found, nil
}

func (c *Client) CreateEndpoint(ctx context.Context, in EndpointInput) (*Endpoint, error) {
	var out Endpoint
	if err := c.doJSON(ctx, http.MethodPost, "/endpoints", in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) UpdateEndpoint(ctx context.Context, id string, in EndpointInput) (*Endpoint, error) {
	var out Endpoint
	if err := c.doJSON(ctx, http.MethodPatch, "/endpoints/"+safeID(id), in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) DeleteEndpoint(ctx context.Context, id string) error {
	// RunPod requires both worker bounds to be zero before endpoint deletion.
	zero := struct {
		WorkersMin int32 `json:"workersMin"`
		WorkersMax int32 `json:"workersMax"`
	}{}
	if err := c.doJSON(ctx, http.MethodPatch, "/endpoints/"+safeID(id), zero, nil); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return c.doJSON(ctx, http.MethodDelete, "/endpoints/"+safeID(id), nil, nil)
}

func safeID(id string) string { return url.PathEscape(id) }

func (c *Client) doJSON(ctx context.Context, method, path string, input, output any) error {
	var payload []byte
	var err error
	if input != nil {
		payload, err = json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode RunPod request: %w", err)
		}
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, reqErr := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.baseURL.String(), "/")+path, bytes.NewReader(payload))
		if reqErr != nil {
			return fmt.Errorf("create RunPod request: %w", reqErr)
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "provider-runpod/clean-room")
		if input != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, doErr := c.httpClient.Do(req)
		if doErr != nil {
			// A failed POST is ambiguous. Surface it immediately; callers recover
			// through an exact-name lookup instead of issuing a second chargeable POST.
			if method == http.MethodPost {
				return fmt.Errorf("%w: %v", ErrCreateAmbiguous, doErr)
			}
			if attempt == maxAttempts-1 {
				return fmt.Errorf("call RunPod API: %w", doErr)
			}
			if err := c.sleep(ctx, backoff(attempt, "")); err != nil {
				return err
			}
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		closeErr := resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read RunPod response: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close RunPod response: %w", closeErr)
		}
		if len(body) > maxBodyBytes {
			return errors.New("RunPod response exceeded 1 MiB limit")
		}

		if resp.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if retryable(resp.StatusCode) && attempt < maxAttempts-1 {
			if err := c.sleep(ctx, backoff(attempt, resp.Header.Get("Retry-After"))); err != nil {
				return err
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return &APIError{StatusCode: resp.StatusCode, RequestID: requestID(resp.Header)}
		}
		if output == nil || len(bytes.TrimSpace(body)) == 0 {
			return nil
		}
		if err := json.Unmarshal(body, output); err != nil {
			return fmt.Errorf("decode RunPod response: %w", err)
		}
		return nil
	}
	return errors.New("RunPod retry budget exhausted")
}

func retryable(status int) bool { return status == http.StatusTooManyRequests || status >= 500 }

func backoff(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
		d := time.Duration(seconds) * time.Second
		if d > 10*time.Second {
			return 10 * time.Second
		}
		return d
	}
	return time.Duration(1<<attempt) * 100 * time.Millisecond
}

func requestID(h http.Header) string {
	for _, key := range []string{"X-Request-Id", "X-Request-ID", "Request-Id"} {
		if v := h.Get(key); v != "" {
			return v
		}
	}
	return ""
}
