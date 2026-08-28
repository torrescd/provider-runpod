// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package inference

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestVerifyHealthModelAndToolCall(t *testing.T) {
	const token = "endpoint-scoped-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("missing auth")
		}
		switch r.URL.Path {
		case "/v2/endpoint_1/health":
			_, _ = io.WriteString(w, `{"workers":{"ready":1}}`)
		case "/v2/endpoint_1/openai/v1/models":
			_, _ = io.WriteString(w, `{"data":[{"id":"org/model"}]}`)
		case "/v2/endpoint_1/openai/v1/chat/completions":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["model"] != "org/model" {
				t.Errorf("model=%v", body["model"])
			}
			_, _ = io.WriteString(w, `{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"call_ready","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}}]}}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	c, err := New("endpoint_1", []byte(token), time.Second, WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.Verify(context.Background(), "org/model")
	if err != nil || !result.Healthy || !result.ModelVerified || !result.ToolCallVerified {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestVerifyErrorsNeverContainResponseOrToken(t *testing.T) {
	const token = "rpa_secret-value"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"token":"`+token+`"}`)
	}))
	defer server.Close()
	c, err := New("endpoint_1", []byte(token), time.Second, WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Verify(context.Background(), "org/model")
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "token") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestProductionTransportHonorsProxyEnvironment(t *testing.T) {
	c, err := New("endpoint_1", []byte("safe-token"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := c.httpClient.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil || transport.ResponseHeaderTimeout != time.Second || transport.MaxResponseHeaderBytes != 64<<10 {
		t.Fatalf("transport is not proxy/header bounded: %+v", transport)
	}
}

func TestVerifyRequiresExactReadinessToolResponse(t *testing.T) {
	cases := map[string]string{
		"missing finish reason": `{"choices":[{"message":{"tool_calls":[{"id":"call_ready","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}}]}}]}`,
		"two choices":           `{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"call_ready","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}}]}},{"finish_reason":"tool_calls","message":{"tool_calls":[]}}]}`,
		"two calls":             `{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"call_ready","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}},{"id":"call_two","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}}]}}]}`,
		"wrong outer type":      `{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"call_ready","type":"custom","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}}]}}]}`,
		"unsafe call ID":        `{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"../../call","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}}]}}]}`,
		"unknown argument":      `{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"call_ready","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\",\"extra\":true}"}}]}}]}`,
		"duplicate argument":    `{"choices":[{"finish_reason":"tool_calls","message":{"tool_calls":[{"id":"call_ready","type":"function","function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\",\"token\":\"ready\"}"}}]}}]}`,
	}
	for name, toolResponse := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/health"):
					_, _ = io.WriteString(w, `{}`)
				case strings.HasSuffix(r.URL.Path, "/models"):
					_, _ = io.WriteString(w, `{"data":[{"id":"org/model"}]}`)
				default:
					_, _ = io.WriteString(w, toolResponse)
				}
			}))
			defer server.Close()
			c, err := New("endpoint_1", []byte("endpoint-token"), time.Second, WithBaseURLForTesting(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			result, err := c.Verify(context.Background(), "org/model")
			if err == nil || result.ToolCallVerified {
				t.Fatalf("unsafe tool response admitted: result=%+v err=%v", result, err)
			}
		})
	}
}

func TestInferenceCredentialNeverFollowsRedirect(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected = true
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	c, err := New("endpoint_1", []byte("inference-key"), time.Second, WithBaseURLForTesting(source.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Verify(context.Background(), "org/model"); err == nil {
		t.Fatal("redirect was not rejected")
	}
	if redirected {
		t.Fatal("inference credential followed redirect to a second origin")
	}
}

func TestInferenceTokenRejectsSurroundingWhitespace(t *testing.T) {
	for _, token := range []string{" endpoint-token", "endpoint-token ", "\tendpoint-token"} {
		if _, err := New("endpoint_1", []byte(token), time.Second); err == nil {
			t.Fatalf("non-canonical inference token %q was accepted", token)
		}
	}
}
