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
			_, _ = io.WriteString(w, `{"choices":[{"message":{"tool_calls":[{"function":{"name":"readiness_probe","arguments":"{\"token\":\"ready\"}"}}]}}]}`)
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
	if !ok || transport.Proxy == nil {
		t.Fatal("transport has no environment proxy function")
	}
}
