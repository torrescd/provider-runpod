// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package inference implements authenticated readiness checks against the
// official RunPod Serverless and OpenAI-compatible endpoints. It deliberately
// has no dependency on management-plane credentials or clients.
package inference

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
	"regexp"
	"strings"
	"time"
)

const maxResponseBytes = 1 << 20

var validEndpointID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

type Result struct {
	Healthy          bool
	ModelVerified    bool
	ToolCallVerified bool
}

type Option func(*Client) error

type Client struct {
	endpointID string
	token      string
	baseURL    *url.URL
	httpClient *http.Client
}

func New(endpointID string, rawToken []byte, timeout time.Duration, opts ...Option) (*Client, error) {
	token := strings.TrimSpace(string(rawToken))
	if !validEndpointID.MatchString(endpointID) {
		return nil, errors.New("invalid RunPod endpoint ID")
	}
	if token == "" || len(token) > 4096 || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("inference token is empty or malformed")
	}
	if timeout <= 0 || timeout > 60*time.Second {
		return nil, errors.New("inference timeout must be between one and sixty seconds")
	}
	u, _ := url.Parse("https://api.runpod.ai")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyFromEnvironment
	c := &Client{endpointID: endpointID, token: token, baseURL: u, httpClient: &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: rejectRedirect}}
	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func rejectRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// WithBaseURLForTesting permits loopback only, preventing token exfiltration
// through a user-configurable production base URL.
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

func (c *Client) Verify(ctx context.Context, expectedModelID string) (Result, error) {
	var result Result
	if expectedModelID == "" {
		return result, errors.New("expected model ID is required")
	}
	if err := c.do(ctx, http.MethodGet, c.endpointPath("/health"), nil, nil); err != nil {
		return result, fmt.Errorf("authenticated health check failed: %w", err)
	}
	result.Healthy = true

	var models struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := c.do(ctx, http.MethodGet, c.openAIPath("/models"), nil, &models); err != nil {
		return result, fmt.Errorf("model identity check failed: %w", err)
	}
	for _, model := range models.Data {
		if model.ID == expectedModelID {
			result.ModelVerified = true
			break
		}
	}
	if !result.ModelVerified {
		return result, errors.New("expected model ID was not advertised")
	}

	request := map[string]any{
		"model":       expectedModelID,
		"messages":    []map[string]string{{"role": "user", "content": "Call readiness_probe with token ready."}},
		"temperature": 0,
		"max_tokens":  64,
		"tools": []any{map[string]any{
			"type": "function",
			"function": map[string]any{
				"name":        "readiness_probe",
				"description": "Return the fixed readiness token.",
				"parameters": map[string]any{
					"type": "object", "additionalProperties": false,
					"properties": map[string]any{"token": map[string]any{"type": "string", "enum": []string{"ready"}}},
					"required":   []string{"token"},
				},
			},
		}},
		"tool_choice": map[string]any{"type": "function", "function": map[string]string{"name": "readiness_probe"}},
	}
	var response struct {
		Choices []struct {
			Message struct {
				ToolCalls []struct {
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := c.do(ctx, http.MethodPost, c.openAIPath("/chat/completions"), request, &response); err != nil {
		return result, fmt.Errorf("tool-call check failed: %w", err)
	}
	for _, choice := range response.Choices {
		for _, call := range choice.Message.ToolCalls {
			if call.Function.Name != "readiness_probe" {
				continue
			}
			var args struct {
				Token string `json:"token"`
			}
			if json.Unmarshal([]byte(call.Function.Arguments), &args) == nil && args.Token == "ready" {
				result.ToolCallVerified = true
			}
		}
	}
	if !result.ToolCallVerified {
		return result, errors.New("model did not return the required readiness tool call")
	}
	return result, nil
}

func (c *Client) endpointPath(suffix string) string {
	return "/v2/" + url.PathEscape(c.endpointID) + suffix
}

func (c *Client) openAIPath(suffix string) string {
	return c.endpointPath("/openai/v1" + suffix)
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var payload []byte
	var err error
	if in != nil {
		payload, err = json.Marshal(in)
		if err != nil {
			return errors.New("encode inference request")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.baseURL.String(), "/")+path, bytes.NewReader(payload))
	if err != nil {
		return errors.New("build inference request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.New("inference endpoint was unreachable")
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes {
		return errors.New("invalid inference response size")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("inference endpoint returned HTTP %d", resp.StatusCode)
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return errors.New("inference endpoint returned invalid JSON")
		}
	}
	return nil
}
