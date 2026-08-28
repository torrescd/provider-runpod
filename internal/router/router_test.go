// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package router

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/credentials"
)

func TestStableModelIsRewrittenAndCredentialIsInjected(t *testing.T) {
	var upstreamModel, upstreamAuth string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upstreamAuth = req.Header.Get("Authorization")
		var body map[string]any
		_ = json.NewDecoder(req.Body).Decode(&body)
		upstreamModel, _ = body["model"].(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"ok"}}]}`)
	}))
	defer upstream.Close()

	r := readyRouter(t, upstream.URL, readyCheck("check-one"))
	models := httptest.NewRecorder()
	r.ServeHTTP(models, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if models.Code != http.StatusOK || !strings.Contains(models.Body.String(), LogicalModelID) {
		t.Fatalf("models response %d %s", models.Code, models.Body.String())
	}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`))
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if response.Code != http.StatusOK || upstreamModel != "org/real-model" || upstreamAuth != "Bearer endpoint-token" {
		t.Fatalf("code=%d model=%q auth=%q", response.Code, upstreamModel, upstreamAuth)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control=%q, want no-store", got)
	}
}

func TestStreamingFlushesAndSignalsResponseLimit(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "12345")
	}))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("bounded-stream"))
	r.responseLimit = 4
	body := `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"stream":true}`
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	result := recorder.Result()
	defer result.Body.Close() //nolint:errcheck
	responseBody, _ := io.ReadAll(result.Body)
	if string(responseBody) != "1234" || result.Trailer.Get("X-Provider-Runpod-Error") != "response-limit-exceeded" {
		t.Fatalf("body=%q trailer=%q", responseBody, result.Trailer.Get("X-Provider-Runpod-Error"))
	}
	if !recorder.Flushed {
		t.Fatal("streaming response was not flushed incrementally")
	}
}

func TestConcurrentInferenceHasNoHiddenQueue(t *testing.T) {
	r := readyRouter(t, "http://127.0.0.1:1", readyCheck("single-flight"))
	started := make(chan struct{})
	release := make(chan struct{})
	upstreamCalls := 0
	r.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		upstreamCalls++
		close(started)
		<-release
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	body := `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	}()
	<-started
	second := httptest.NewRecorder()
	r.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("concurrent request status=%d, want immediate 429", second.Code)
	}
	if upstreamCalls != 1 {
		t.Fatalf("concurrent request reached upstream: calls=%d", upstreamCalls)
	}
	close(release)
	<-firstDone
}

func TestCollisionAndInboundSecretsFailClosed(t *testing.T) {
	scheme := testScheme(t)
	one, two := readyCheck("one"), readyCheck("two")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(one, two, readyRouterEndpoint(), readyRouterTemplate(), inferenceSecret()).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if recorder.Code != http.StatusServiceUnavailable || !strings.Contains(recorder.Body.String(), "multiple admitted") {
		t.Fatalf("collision was not closed: %d %s", recorder.Code, recorder.Body.String())
	}

	r = readyRouter(t, "http://127.0.0.1:1", readyCheck("one"))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"runpod-experiment"}`))
	req.Header.Set("Authorization", "Bearer must-not-forward")
	recorder = httptest.NewRecorder()
	r.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("credential accepted: %d", recorder.Code)
	}
}

func TestProcessHealthDoesNotDependOnAnAdmittedRoute(t *testing.T) {
	scheme := testScheme(t)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}

	health := httptest.NewRecorder()
	r.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("process health without a route=%d, want 200", health.Code)
	}

	ready := httptest.NewRecorder()
	r.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("route readiness without a route=%d, want 503", ready.Code)
	}
}

func TestReadyCheckWithoutDrainFinalizerCannotRoute(t *testing.T) {
	check := readyCheck("missing-drain")
	check.Finalizers = nil
	r := readyRouter(t, "http://127.0.0.1:1", check)
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("EndpointCheck without router drain finalizer was admitted")
	}
}

func TestCredentialMaterialIsRejectedBeforeProxying(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { upstreamCalls++ }))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("one"))
	cases := map[string]string{
		"RunPod key":         `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"rpa_abcdefghijk"}]}`,
		"escaped RunPod key": `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"rpa_\u0061bcdefghijk"}]}`,
		"Bearer token":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"Bearer abcdefghijklmnop"}]}`,
		"GitHub token":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"github_pat_abcdefghijklmnopqrstuvwxyz"}]}`,
		"AWS access key":     `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"AKIA1234567890ABCDEF"}]}`,
		"private key":        `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"-----BEGIN PRIVATE KEY-----"}]}`,
		"JWT":                `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"eyJabcdefghijk.abcdefghijk.abcdefghijk"}]}`,
		"structured secret":  `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"metadata":{"credentials":"must-not-leave"}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("credential material response=%d, want 400", recorder.Code)
			}
		})
	}
	if upstreamCalls != 0 {
		t.Fatalf("credential-bearing input reached upstream %d times", upstreamCalls)
	}
}

func TestToolCallArgumentsAreStrictlyDecodedAndSecretScanned(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { upstreamCalls++ }))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("tool-arguments"))
	const prefix = `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"inspect","arguments":`
	const suffix = `}}]}],"tools":[{"type":"function","function":{"name":"inspect","description":"Inspect a bounded value.","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"value":{"type":"string"}},"required":["value"]}}}]}`
	cases := map[string]string{
		"secret-labelled argument": `"{\"token\":\"plain-secret\"}"`,
		"escaped credential":       `"{\"value\":\"rpa_\\u0061bcdefghijk\"}"`,
		"duplicate argument key":   `"{\"value\":\"one\",\"value\":\"two\"}"`,
		"trailing JSON":            `"{\"value\":\"one\"}{}"`,
		"non-object":               `"[\"value\"]"`,
	}
	for name, arguments := range cases {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(prefix+arguments+suffix)))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("response=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if upstreamCalls != 0 {
		t.Fatalf("invalid tool arguments reached upstream %d times", upstreamCalls)
	}
}

func TestMultimodalAndURLBearingInputsNeverReachUpstream(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("text-only"))
	cases := map[string]string{
		"image URL":               `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"http://169.254.169.254/latest/meta-data"}}]}]}`,
		"data image":              `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}]}`,
		"input audio":             `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AAAA","format":"wav"}}]}]}`,
		"nested video URL":        `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":{"nested":{"video_url":"https://redirect.example/video"}}}]}`,
		"top-level file URL":      `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"file_url":"https://redirect.example/file"}`,
		"generic extension URL":   `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"extension":{"url":"http://127.0.0.1/admin"}}`,
		"audio modality":          `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"modalities":["text","audio"]}`,
		"chat template":           `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"chat_template":"unsafe"}`,
		"media kwargs":            `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"media_io_kwargs":{}}`,
		"multimodal kwargs":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"mm_processor_kwargs":{}}`,
		"documents":               `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"documents":[]}`,
		"response format":         `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"response_format":{"type":"json_object"}}`,
		"structured outputs":      `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"structured_outputs":{}}`,
		"reasoning":               `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"reasoning":{"effort":"high"}}`,
		"thinking":                `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"thinking":true}`,
		"log probabilities":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"logprobs":true}`,
		"beam search":             `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"beam_search":true}`,
		"ignore eos":              `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"ignore_eos":true}`,
		"minimum tokens":          `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"min_tokens":4}`,
		"priority":                `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"priority":-1}`,
		"KV cache extension":      `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"kv_transfer_params":{}}`,
		"xargs extension":         `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"xargs":{}}`,
		"multiple completions":    `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"n":2}`,
		"excess output":           `{"model":"runpod-experiment","max_tokens":4097,"messages":[{"role":"user","content":"hello"}]}`,
		"custom role":             `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"developer","content":"hello"}]}`,
		"remote schema ref":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"$ref":"https://attacker.invalid/schema"}}}]}`,
		"alternate schema URI":    `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2019-09/schema","type":"object"}}}]}`,
		"nested schema URI":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"type":"object","properties":{"value":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string"}}}}}]}`,
		"schema identifier":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"$id":"https://attacker.invalid/schema","type":"object"}}}]}`,
		"complex schema default":  `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"type":"object","properties":{"mode":{"type":"string","default":{"remote":"https://attacker.invalid"}}}}}}]}`,
		"function strict":         `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","strict":true,"parameters":{"type":"object"}}}]}`,
		"missing description":     `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}}}]}`,
		"missing parameters":      `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe"}}]}`,
		"missing schema dialect":  `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"type":"object"}}}]}`,
		"missing schema type":     `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema"}}}]}`,
		"properties on string":    `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","properties":{}}}}]}`,
		"items on object":         `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","items":{"type":"string"}}}}]}`,
		"numeric bound on string": `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","minimum":1}}}]}`,
		"empty object schema":     `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{},"required":[]}}}]}`,
		"object missing required": `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"value":{"type":"string"}}}}}]}`,
		"enum type mismatch":      `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"value":{"type":"string","enum":[1]}},"required":["value"]}}}]}`,
		"object enum":             `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"value":{"type":"object","properties":{"nested":{"type":"string"}},"required":["nested"],"enum":[null]}},"required":["value"]}}}]}`,
		"default type mismatch":   `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","description":"unsafe","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"value":{"type":"string","default":1}},"required":["value"]}}}]}`,
		"additional properties":   `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"type":"object","additionalProperties":false}}}]}`,
		"schema const":            `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"type":"string","const":"x"}}}]}`,
		"exclusive maximum":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"type":"number","exclusiveMaximum":2}}}]}`,
		"string bound":            `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"type":"string","maxLength":8}}}]}`,
		"array bound":             `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"type":"array","minItems":1,"items":{"type":"string"}}}}]}`,
		"schema combinator":       `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"unsafe","parameters":{"oneOf":[{"type":"string"},{"type":"number"}]}}}]}`,
		"deferred tool loading":   `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","defer_loading":true,"function":{"name":"unsafe","parameters":{"type":"object"}}}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("response=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if upstreamCalls != 0 {
		t.Fatalf("multimodal/URL-bearing input reached upstream %d times", upstreamCalls)
	}
}

func TestPinnedOpenCode11823TextPartsAndToolSchemaReachUpstream(t *testing.T) {
	upstreamCalls := 0
	forwardedSchema := map[string]any{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upstreamCalls++
		var forwarded map[string]any
		if err := json.NewDecoder(req.Body).Decode(&forwarded); err != nil {
			t.Errorf("decode forwarded OpenCode fixture: %v", err)
		}
		tools := forwarded["tools"].([]any)
		forwardedSchema = tools[0].(map[string]any)["function"].(map[string]any)["parameters"].(map[string]any)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("text-parts"))
	body := `{"model":"runpod-experiment","max_tokens":4096,"temperature":0.2,"top_p":1,"stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"system","content":"Use tools safely."},{"role":"user","content":[{"type":"text","text":"explain https://example.invalid as plain text"}]},{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"inspect","arguments":"{\"url\":\"https://example.invalid\"}"}}]},{"role":"tool","tool_call_id":"call_1","content":"ok"}],"tools":[{"type":"function","function":{"name":"inspect","description":"Captured OpenCode v1.18.23 tool shape.","parameters":{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"format":{"type":"string","enum":["markdown","text"],"default":"markdown"}},"required":["format"]}}}],"tool_choice":{"type":"function","function":{"name":"inspect"}}}`
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if recorder.Code != http.StatusOK || upstreamCalls != 1 {
		t.Fatalf("text-only tool request response=%d upstreamCalls=%d body=%s", recorder.Code, upstreamCalls, recorder.Body.String())
	}
	if _, leaked := forwardedSchema["$schema"]; leaked {
		t.Fatal("validated OpenCode $schema metadata was forwarded to the worker")
	}
	properties := forwardedSchema["properties"].(map[string]any)
	if _, leaked := properties["format"].(map[string]any)["default"]; leaked {
		t.Fatal("validated OpenCode default annotation was forwarded to the worker")
	}
}

func TestSanitizedPinnedOpenCode11823CaptureShapeAndForwarding(t *testing.T) {
	const fixtureSHA256 = "7d0f8beea4f70e98bf8aa66720f29af5f02b62e99ccb5ebe842d357d71388c57"
	body, err := os.ReadFile("testdata/opencode-1.18.23-chat-completions.sanitized.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(body)); got != fixtureSHA256 {
		t.Fatalf("sanitized OpenCode capture digest=%s, want %s", got, fixtureSHA256)
	}
	var captured map[string]any
	if err := json.Unmarshal(body, &captured); err != nil {
		t.Fatal(err)
	}
	toolNames, nodes, depth, keywords, types := capturedSchemaShape(t, captured)
	if !slices.Equal(toolNames, []string{"bash", "edit", "glob", "grep", "read", "skill", "task", "todowrite", "webfetch", "write"}) {
		t.Fatalf("captured tool names=%v", toolNames)
	}
	if nodes != 41 || depth != 3 {
		t.Fatalf("captured schema nodes=%d depth=%d, want 41/3", nodes, depth)
	}
	if !reflect.DeepEqual(keywords, map[string]bool{"$schema": true, "default": true, "description": true, "enum": true, "exclusiveMinimum": true, "items": true, "maximum": true, "minimum": true, "properties": true, "required": true, "type": true}) {
		t.Fatalf("captured schema keywords=%v", keywords)
	}
	if !reflect.DeepEqual(types, map[string]bool{"array": true, "boolean": true, "integer": true, "number": true, "object": true, "string": true}) {
		t.Fatalf("captured schema types=%v", types)
	}

	var expected map[string]any
	if err := json.Unmarshal(body, &expected); err != nil {
		t.Fatal(err)
	}
	expected["model"] = "org/real-model"
	stripCapturedAnnotations(expected)
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		upstreamCalls++
		var forwarded map[string]any
		if err := json.NewDecoder(req.Body).Decode(&forwarded); err != nil {
			t.Errorf("decode forwarded capture: %v", err)
		}
		if !reflect.DeepEqual(forwarded, expected) {
			t.Error("router changed fields beyond model rewrite and validated annotation stripping")
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("captured-opencode"))
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body))))
	if recorder.Code != http.StatusOK || upstreamCalls != 1 {
		t.Fatalf("captured OpenCode response=%d upstreamCalls=%d body=%s", recorder.Code, upstreamCalls, recorder.Body.String())
	}
}

func TestPinnedOpenCodeTitleRequestIsAccepted(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		_, _ = io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("opencode-title"))
	body := `{"model":"runpod-experiment","max_tokens":80,"messages":[{"role":"system","content":"Create a title."},{"role":"user","content":"first"},{"role":"user","content":"second"}],"stream":true,"stream_options":{"include_usage":true}}`
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if recorder.Code != http.StatusOK || upstreamCalls != 1 {
		t.Fatalf("OpenCode title response=%d calls=%d body=%s", recorder.Code, upstreamCalls, recorder.Body.String())
	}
}

func TestAggregateSchemaBudgetAndCapturedDepthFailBeforeUpstream(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { upstreamCalls++ }))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("schema-budget"))

	top := func(tools []any) map[string]any {
		return map[string]any{
			"model": LogicalModelID, "max_tokens": float64(64),
			"messages": []any{map[string]any{"role": "user", "content": "hello"}},
			"tools":    tools,
		}
	}
	tools := make([]any, 0, maxTools)
	for i := 0; i < maxTools; i++ {
		properties := map[string]any{}
		for j := 0; j < 4; j++ {
			properties[fmt.Sprintf("p%d", j)] = map[string]any{"type": "string"}
		}
		tools = append(tools, map[string]any{"type": "function", "function": map[string]any{
			"name": fmt.Sprintf("tool_%d", i), "description": "bounded",
			"parameters": map[string]any{"$schema": openCodeSchemaURI, "type": "object", "properties": properties},
		}})
	}
	deep := map[string]any{"type": "string"}
	for i := 0; i < 4; i++ {
		deep = map[string]any{"type": "object", "properties": map[string]any{"next": deep}}
	}
	deep["$schema"] = openCodeSchemaURI
	deepTools := []any{map[string]any{"type": "function", "function": map[string]any{"name": "deep", "description": "bounded", "parameters": deep}}}

	for name, payload := range map[string]map[string]any{"aggregate": top(tools), "depth four": top(deepTools)} {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(string(body))))
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("schema budget response=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if upstreamCalls != 0 {
		t.Fatalf("oversized schema grammar reached upstream %d times", upstreamCalls)
	}
}

func capturedSchemaShape(t *testing.T, payload map[string]any) ([]string, int, int, map[string]bool, map[string]bool) {
	t.Helper()
	names := []string{}
	nodes, maxDepth := 0, 0
	keywords, types := map[string]bool{}, map[string]bool{}
	var walk func(any, int)
	walk = func(raw any, depth int) {
		schema := raw.(map[string]any)
		nodes++
		if depth > maxDepth {
			maxDepth = depth
		}
		for key := range schema {
			keywords[key] = true
		}
		if typeName, ok := schema["type"].(string); ok {
			types[typeName] = true
		}
		if properties, ok := schema["properties"].(map[string]any); ok {
			for _, child := range properties {
				walk(child, depth+1)
			}
		}
		if items, ok := schema["items"]; ok {
			walk(items, depth+1)
		}
	}
	for _, raw := range payload["tools"].([]any) {
		function := raw.(map[string]any)["function"].(map[string]any)
		names = append(names, function["name"].(string))
		walk(function["parameters"], 0)
	}
	return names, nodes, maxDepth, keywords, types
}

func stripCapturedAnnotations(payload map[string]any) {
	var walk func(any)
	walk = func(raw any) {
		schema := raw.(map[string]any)
		delete(schema, "$schema")
		delete(schema, "default")
		if properties, ok := schema["properties"].(map[string]any); ok {
			for _, child := range properties {
				walk(child)
			}
		}
		if items, ok := schema["items"]; ok {
			walk(items)
		}
	}
	for _, raw := range payload["tools"].([]any) {
		walk(raw.(map[string]any)["function"].(map[string]any)["parameters"])
	}
}

func TestRouterCredentialNeverFollowsRedirect(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected = true
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	r := readyRouter(t, source.URL, readyCheck("one"))
	recorder := httptest.NewRecorder()
	body := `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if recorder.Code != http.StatusFound {
		t.Fatalf("redirect response=%d, want 302", recorder.Code)
	}
	if redirected {
		t.Fatal("router inference credential followed redirect to a second origin")
	}
}

func TestDeletingCheckWithdrawsRouteBeforeFinalizerAck(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("deleting")
	now := metav1.Now()
	check.DeletionTimestamp = &now
	check.Finalizers = []string{RouterDrainFinalizer}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, inferenceSecret()).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("deleting check unexpectedly routed")
	}
	remaining := &verificationv1alpha1.EndpointCheck{}
	err = kube.Get(context.Background(), types.NamespacedName{Namespace: check.Namespace, Name: check.Name}, remaining)
	if err == nil && len(remaining.Finalizers) != 0 {
		t.Fatalf("drain finalizer remained: %v", remaining.Finalizers)
	}
	if r.snapshot() != nil {
		t.Fatal("route remained active")
	}
}

func TestDeletingCheckCancelsAndDrainsInflightRequestBeforeFinalizerAck(t *testing.T) {
	check := readyCheck("inflight")
	check.Finalizers = []string{RouterDrainFinalizer}
	r := readyRouter(t, "http://127.0.0.1:1", check)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	allowExit := make(chan struct{})
	r.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: &cancellationBlockingBody{
				ctx: req.Context(), cancelled: cancelled, allowExit: allowExit,
			},
		}, nil
	})}

	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		body := `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	}()
	<-started

	current := &verificationv1alpha1.EndpointCheck{}
	key := types.NamespacedName{Namespace: check.Namespace, Name: check.Name}
	if err := r.kube.Get(context.Background(), key, current); err != nil {
		t.Fatal(err)
	}
	if err := r.kube.Delete(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- r.Refresh(context.Background()) }()
	<-cancelled

	terminating := &verificationv1alpha1.EndpointCheck{}
	if err := r.kube.Get(context.Background(), key, terminating); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(terminating.Finalizers, RouterDrainFinalizer) {
		t.Fatal("drain finalizer was acknowledged while an upstream request still held a route lease")
	}
	newRequest := httptest.NewRecorder()
	newDone := make(chan struct{})
	go func() {
		r.ServeHTTP(newRequest, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		close(newDone)
	}()

	close(allowExit)
	<-requestDone
	_ = <-refreshDone // No eligible route is the expected result.
	<-newDone
	if newRequest.Code != http.StatusServiceUnavailable {
		t.Fatalf("post-withdraw request status=%d, want 503", newRequest.Code)
	}
	remaining := &verificationv1alpha1.EndpointCheck{}
	if err := r.kube.Get(context.Background(), key, remaining); err == nil && slices.Contains(remaining.Finalizers, RouterDrainFinalizer) {
		t.Fatal("drain finalizer remained after the in-flight request exited")
	}
}

func TestUnchangedRefreshPreservesInflightRoute(t *testing.T) {
	check := readyCheck("unchanged")
	r := readyRouter(t, "http://127.0.0.1:1", check)
	if err := r.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	cancelled := make(chan struct{})
	allowExit := make(chan struct{})
	r.httpClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		close(started)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: &unchangedBlockingBody{
				ctx: req.Context(), cancelled: cancelled, allowExit: allowExit,
			},
		}, nil
	})}

	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		body := `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	}()
	<-started
	refreshDone := make(chan error, 1)
	go func() { refreshDone <- r.Refresh(context.Background()) }()
	select {
	case err := <-refreshDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("unchanged refresh blocked on an in-flight request")
	}
	select {
	case <-cancelled:
		t.Fatal("unchanged refresh cancelled an in-flight request")
	default:
	}
	close(allowExit)
	<-requestDone
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

type cancellationBlockingBody struct {
	ctx       context.Context
	cancelled chan struct{}
	allowExit chan struct{}
	once      sync.Once
}

func (b *cancellationBlockingBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	b.once.Do(func() { close(b.cancelled) })
	<-b.allowExit
	return 0, b.ctx.Err()
}

func (*cancellationBlockingBody) Close() error { return nil }

type unchangedBlockingBody struct {
	ctx       context.Context
	cancelled chan struct{}
	allowExit chan struct{}
	once      sync.Once
}

func (b *unchangedBlockingBody) Read([]byte) (int, error) {
	select {
	case <-b.ctx.Done():
		b.once.Do(func() { close(b.cancelled) })
		return 0, b.ctx.Err()
	case <-b.allowExit:
		return 0, io.EOF
	}
}

func (*unchangedBlockingBody) Close() error { return nil }

func TestExpiredCheckAndProxyTransportFailClosed(t *testing.T) {
	check := readyCheck("expired")
	check.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	r := readyRouter(t, "http://127.0.0.1:1", check)
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("expired check routed")
	}
	transport, ok := r.httpClient.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil || transport.ResponseHeaderTimeout != 10*time.Minute || transport.MaxResponseHeaderBytes != 64<<10 {
		t.Fatalf("router transport is not proxy/header bounded: %+v", transport)
	}
}

func TestRouterForwardsOnlyBoundedSafeUpstreamRequestID(t *testing.T) {
	for name, requestID := range map[string]string{
		"safe":  "req_123:abc",
		"long":  strings.Repeat("a", 129),
		"space": "request id",
	} {
		t.Run(name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Request-Id", requestID)
				_, _ = io.WriteString(w, `{}`)
			}))
			defer upstream.Close()
			r := readyRouter(t, upstream.URL, readyCheck("request-id"))
			recorder := httptest.NewRecorder()
			body := `{"model":"runpod-experiment","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`
			r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
			got := recorder.Header().Get("X-Request-Id")
			if (name == "safe" && got != requestID) || (name != "safe" && got != "") {
				t.Fatalf("forwarded request ID=%q", got)
			}
		})
	}
}

func TestStaleVerificationAndCredentialRotationFailClosed(t *testing.T) {
	check := readyCheck("stale-generation")
	check.Generation = 2
	check.Status.ObservedGeneration = 1
	r := readyRouter(t, "http://127.0.0.1:1", check)
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("status from a prior generation was routed")
	}

	check = readyCheck("rotated-secret")
	check.Status.AtProvider.CredentialsSecretResourceVersion = "old"
	r = readyRouter(t, "http://127.0.0.1:1", check)
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("credential version not used by readiness checks was routed")
	}
}

func TestExternalNameAnnotationTamperingNeverReachesRouterUpstream(t *testing.T) {
	for name, tamper := range map[string]func(context.Context, *Router) error{
		"Endpoint annotation": func(ctx context.Context, r *Router) error {
			endpoint := &serverlessv1alpha1.Endpoint{}
			key := types.NamespacedName{Namespace: "runpod-system", Name: "endpoint"}
			if err := r.kube.Get(ctx, key, endpoint); err != nil {
				return err
			}
			meta.SetExternalName(endpoint, "endpoint_tampered")
			return r.kube.Update(ctx, endpoint)
		},
		"Template annotation": func(ctx context.Context, r *Router) error {
			template := &serverlessv1alpha1.Template{}
			key := types.NamespacedName{Namespace: "runpod-system", Name: "template"}
			if err := r.kube.Get(ctx, key, template); err != nil {
				return err
			}
			meta.SetExternalName(template, "template_tampered")
			return r.kube.Update(ctx, template)
		},
	} {
		t.Run(name, func(t *testing.T) {
			upstreamCalls := 0
			upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { upstreamCalls++ }))
			defer upstream.Close()
			r := readyRouter(t, upstream.URL, readyCheck("tamper"))
			if err := tamper(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			body := `{"model":"runpod-experiment","max_tokens":1,"messages":[{"role":"user","content":"hello"}]}`
			r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
			if upstreamCalls != 0 || recorder.Code == http.StatusOK {
				t.Fatalf("annotation tamper response=%d reached upstream=%d", recorder.Code, upstreamCalls)
			}
		})
	}
}

func TestReferencedEndpointRolloutRevisionMustMatchVerification(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("rollout")
	check.Spec.ForProvider.EndpointIDRef = &xpv2.Reference{Name: "endpoint"}
	check.Status.AtProvider.EndpointResourceUID = "endpoint-uid"
	check.Status.AtProvider.EndpointResourceGeneration = 1
	check.Status.AtProvider.EndpointVersion = routerInt32Ptr(7)
	endpoint := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{
		Name: "endpoint", Namespace: check.Namespace, UID: "endpoint-uid", Generation: 1,
	}}
	meta.SetExternalName(endpoint, "endpoint_1")
	endpoint.Status.ObservedGeneration = 1
	endpoint.Status.AtProvider.ID = "endpoint_1"
	endpoint.Status.AtProvider.TemplateID = "template_1"
	endpoint.Status.AtProvider.Version = routerInt32Ptr(8)
	endpoint.Status.AtProvider.FlashBootDisabled = true
	endpoint.Status.AtProvider.FlashBootEvidenceVersion = routerInt32Ptr(8)
	endpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.Now()
	endpoint.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, endpoint, inferenceSecret()).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("route admitted a model verification from the prior RunPod rollout version")
	}
}

func TestRouterRejectsExpiredFlashBootEvidence(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("expired-flashboot-proof")
	endpoint := readyRouterEndpoint()
	endpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.NewTime(time.Now().Add(-serverlessv1alpha1.FlashBootEvidenceMaxAge - time.Second))
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, endpoint, inferenceSecret()).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("route admitted expired FlashBoot write evidence")
	}
}

func TestRouterWithdrawsAfterEmptyWorkerObservationClearsProof(t *testing.T) {
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		_, _ = io.WriteString(w, `{}`)
	}))
	defer upstream.Close()
	r := readyRouter(t, upstream.URL, readyCheck("cold-start-proof"))
	if err := r.Refresh(context.Background()); err != nil || r.snapshot() == nil {
		t.Fatalf("initial bounded worker proof did not admit route: err=%v", err)
	}

	endpoint := &serverlessv1alpha1.Endpoint{}
	key := types.NamespacedName{Namespace: "runpod-system", Name: "endpoint"}
	if err := r.kube.Get(context.Background(), key, endpoint); err != nil {
		t.Fatal(err)
	}
	endpoint.Status.AtProvider.WorkerSecurityValidated = false
	endpoint.Status.AtProvider.WorkerSecurityProofVersion = nil
	endpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.Time{}
	if err := r.kube.Update(context.Background(), endpoint); err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("route survived a direct zero-worker observation that cleared worker proof")
	}

	body := `{"model":"runpod-experiment","max_tokens":1,"messages":[{"role":"user","content":"wake"}]}`
	recorder := httptest.NewRecorder()
	r.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body)))
	if upstreamCalls != 0 || recorder.Code == http.StatusOK {
		t.Fatalf("unproven cold worker reached upstream: status=%d calls=%d", recorder.Code, upstreamCalls)
	}
}

func TestRouterRejectsLiveCheckWhoseReferencedEndpointExpired(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("expired-endpoint")
	endpoint := readyRouterEndpoint()
	endpoint.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	endpoint.Spec.ForProvider.MaxLifetimeSeconds = 3600
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, endpoint, readyRouterTemplate(), inferenceSecret()).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("live EndpointCheck routed past the referenced Endpoint hard lifetime")
	}
}

func TestRouterRejectsEndpointBoundToPriorTemplateRevision(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("stale-template")
	endpoint := readyRouterEndpoint()
	template := readyRouterTemplate()
	template.Generation = 2
	template.Status.ObservedGeneration = 2
	template.Spec.ForProvider.ImageName = "registry.example/model@sha256:" + strings.Repeat("b", 64)
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, endpoint, template, inferenceSecret()).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("route admitted an Endpoint bound to the prior Template revision")
	}
}

func TestManagementPurposeCredentialFailsClosed(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("wrong-purpose")
	secret := inferenceSecret()
	secret.Labels[credentials.PurposeLabel] = credentials.PurposeManagement
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, readyRouterEndpoint(), readyRouterTemplate(), secret).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("management-purpose credential was routed")
	}
}

func TestRouterRejectsWhitespacePaddedInferenceToken(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("whitespace-token")
	secret := inferenceSecret()
	secret.Data["token"] = []byte(" endpoint-token ")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, readyRouterEndpoint(), readyRouterTemplate(), secret).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("whitespace-padded inference token was routed")
	}
}

func TestRefreshUsesDirectReaderForInferenceSecret(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("direct-secret")
	cached := fake.NewClientBuilder().WithScheme(scheme).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, readyRouterEndpoint(), readyRouterTemplate(), inferenceSecret()).Build()
	r, err := New(cached, "runpod-system", WithAPIReader(direct))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err != nil || r.snapshot() == nil {
		t.Fatalf("direct Secret reader was not used: route=%v err=%v", r.snapshot(), err)
	}
}

func TestConcurrentRefreshCannotReactivateWithdrawnRoute(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("aba")
	check.Finalizers = []string{RouterDrainFinalizer}
	backing := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, readyRouterEndpoint(), readyRouterTemplate(), inferenceSecret()).Build()
	reader := &blockingFirstEndpointCheckListReader{
		Reader:  backing,
		listed:  make(chan struct{}),
		release: make(chan struct{}),
	}
	r, err := New(backing, "runpod-system", WithAPIReader(reader))
	if err != nil {
		t.Fatal(err)
	}
	r.replace(&route{checkName: "prior", token: []byte("prior-token")}, "")

	firstDone := make(chan error, 1)
	go func() { firstDone <- r.Refresh(context.Background()) }()
	<-reader.listed // The first refresh now holds an eligible but stale snapshot.

	current := &verificationv1alpha1.EndpointCheck{}
	key := types.NamespacedName{Namespace: check.Namespace, Name: check.Name}
	if err := backing.Get(context.Background(), key, current); err != nil {
		t.Fatal(err)
	}
	if err := backing.Delete(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- r.Refresh(context.Background()) }()
	close(reader.release)

	if err := <-firstDone; err == nil {
		t.Fatal("stale refresh committed instead of detecting the deletion epoch")
	}
	_ = <-secondDone // No eligible route is the expected steady state.
	if r.snapshot() != nil {
		t.Fatal("an older eligible snapshot reactivated the withdrawn route")
	}
	remaining := &verificationv1alpha1.EndpointCheck{}
	if err := backing.Get(context.Background(), key, remaining); err == nil && len(remaining.Finalizers) != 0 {
		t.Fatalf("drain finalizer remained after route withdrawal: %v", remaining.Finalizers)
	}
}

type blockingFirstEndpointCheckListReader struct {
	client.Reader
	once    sync.Once
	listed  chan struct{}
	release chan struct{}
}

func (r *blockingFirstEndpointCheckListReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err := r.Reader.List(ctx, list, opts...); err != nil {
		return err
	}
	if _, ok := list.(*verificationv1alpha1.EndpointCheckList); !ok {
		return nil
	}
	blocked := false
	r.once.Do(func() {
		blocked = true
		close(r.listed)
	})
	if blocked {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.release:
		}
	}
	return nil
}

func readyRouter(t *testing.T, upstream string, checks ...*verificationv1alpha1.EndpointCheck) *Router {
	t.Helper()
	scheme := testScheme(t)
	objects := make([]runtime.Object, 0, len(checks)+1)
	for _, c := range checks {
		objects = append(objects, c)
	}
	objects = append(objects, readyRouterEndpoint())
	objects = append(objects, readyRouterTemplate())
	objects = append(objects, inferenceSecret())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()
	r, err := New(kube, "runpod-system", WithUpstreamBaseForTesting(upstream))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func readyCheck(name string) *verificationv1alpha1.EndpointCheck {
	check := &verificationv1alpha1.EndpointCheck{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "runpod-system", CreationTimestamp: metav1.Now(), Finalizers: []string{RouterDrainFinalizer}},
		Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{
			MaxLifetimeSeconds: 3600, EndpointIDRef: &xpv2.Reference{Name: "endpoint"}, ExpectedModelID: "org/real-model",
			VerificationIntervalSeconds:   3600,
			InferenceCredentialsSecretRef: xpv2.LocalSecretKeySelector{LocalSecretReference: xpv2.LocalSecretReference{Name: "inference"}, Key: "token"},
		}},
		Status: verificationv1alpha1.EndpointCheckStatus{AtProvider: verificationv1alpha1.EndpointCheckObservation{
			EndpointID: "endpoint_1", EndpointResourceUID: "endpoint-uid", EndpointResourceGeneration: 1, EndpointVersion: routerInt32Ptr(1),
			TemplateResourceUID: "template-uid", TemplateResourceGeneration: 1, TemplateImageDigest: testTemplateImage,
			Healthy: true, ModelVerified: true, ToolCallVerified: true,
			CredentialsSecretResourceVersion: "1", LastCheckedAt: metav1.Now(), LastVerifiedAt: metav1.Now(),
		}},
	}
	check.Status.ObservedGeneration = check.Generation
	check.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	return check
}

func readyRouterEndpoint() *serverlessv1alpha1.Endpoint {
	endpoint := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{
		Name: "endpoint", Namespace: "runpod-system", UID: "endpoint-uid", Generation: 1, CreationTimestamp: metav1.Now(),
	}, Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{MaxLifetimeSeconds: 3600, TemplateIDRef: &xpv2.Reference{Name: "template"}}}}
	meta.SetExternalName(endpoint, "endpoint_1")
	endpoint.Status.ObservedGeneration = 1
	endpoint.Status.AtProvider.ID = "endpoint_1"
	endpoint.Status.AtProvider.TemplateID = "template_1"
	endpoint.Status.AtProvider.Version = routerInt32Ptr(1)
	endpoint.Status.AtProvider.TemplateResourceUID = "template-uid"
	endpoint.Status.AtProvider.TemplateResourceGeneration = 1
	endpoint.Status.AtProvider.TemplateImageDigest = testTemplateImage
	endpoint.Status.AtProvider.FlashBootDisabled = true
	endpoint.Status.AtProvider.FlashBootEvidenceVersion = routerInt32Ptr(1)
	endpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.Now()
	endpoint.Status.AtProvider.WorkerSecurityValidated = true
	endpoint.Status.AtProvider.WorkerSecurityProofVersion = routerInt32Ptr(1)
	endpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.Now()
	endpoint.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	return endpoint
}

const testTemplateImage = "registry.example/model@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func readyRouterTemplate() *serverlessv1alpha1.Template {
	template := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system", UID: "template-uid", Generation: 1},
		Spec:       serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{ImageName: testTemplateImage}},
	}
	meta.SetExternalName(template, "template_1")
	template.Status.ObservedGeneration = 1
	template.Status.AtProvider.ID = "template_1"
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	return template
}

func inferenceSecret() *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "inference", Namespace: "runpod-system", ResourceVersion: "1",
		Labels: map[string]string{credentials.PurposeLabel: credentials.PurposeInference},
	}, Data: map[string][]byte{"token": []byte("endpoint-token")}}
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := verificationv1alpha1.SchemeBuilder.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := serverlessv1alpha1.SchemeBuilder.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func routerInt32Ptr(value int32) *int32 { return &value }
