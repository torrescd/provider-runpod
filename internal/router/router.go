// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package router provides a single fail-closed OpenAI-compatible route backed
// by one admitted EndpointCheck. It has no management-client dependency.
package router

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
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
)

const (
	LogicalModelID       = "runpod-experiment"
	RouterDrainFinalizer = "router.runpod.crossplane.io/drain"
	maxRequestBytes      = 2 << 20
)

var secretPattern = regexp.MustCompile(`(?i)rpa_[a-z0-9_-]{8,}`)

type route struct {
	namespace       string
	checkName       string
	endpointID      string
	expectedModelID string
	token           []byte
}

type Option func(*Router) error

type Router struct {
	kube         client.Client
	namespace    string
	upstreamBase *url.URL
	httpClient   *http.Client
	mu           sync.RWMutex
	active       *route
	stateError   string
}

func New(kube client.Client, namespace string, opts ...Option) (*Router, error) {
	if kube == nil || namespace == "" {
		return nil, errors.New("Kubernetes client and namespace are required")
	}
	u, _ := url.Parse("https://api.runpod.ai")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyFromEnvironment
	r := &Router{
		kube: kube, namespace: namespace, upstreamBase: u,
		httpClient: &http.Client{Transport: transport, Timeout: 10 * time.Minute},
		stateError: "no admitted EndpointCheck",
	}
	for _, o := range opts {
		if err := o(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// WithUpstreamBaseForTesting permits loopback only.
func WithUpstreamBaseForTesting(raw string) Option {
	return func(r *Router) error {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return errors.New("invalid test upstream URL")
		}
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("test upstream URL must be loopback")
		}
		r.upstreamBase = u
		return nil
	}
}

// Refresh rebuilds state from Kubernetes. Any read error, route collision,
// stale check, or malformed Secret withdraws the route and erases its token.
func (r *Router) Refresh(ctx context.Context) error {
	checks := &verificationv1alpha1.EndpointCheckList{}
	if err := r.kube.List(ctx, checks, client.InNamespace(r.namespace)); err != nil {
		r.withdraw("cannot read EndpointChecks")
		return err
	}
	eligible := make([]*verificationv1alpha1.EndpointCheck, 0, 1)
	deleting := make([]*verificationv1alpha1.EndpointCheck, 0)
	now := time.Now()
	for i := range checks.Items {
		check := &checks.Items[i]
		if !check.DeletionTimestamp.IsZero() {
			deleting = append(deleting, check)
			continue
		}
		expiresAt := check.CreationTimestamp.Add(time.Duration(check.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
		ready := check.Status.GetCondition(xpv2.TypeReady).Status == corev1.ConditionTrue
		observed := check.Status.AtProvider
		if ready && now.Before(expiresAt) && observed.Healthy && observed.ModelVerified &&
			observed.ToolCallVerified && observed.EndpointID != "" {
			eligible = append(eligible, check)
		}
	}

	var next *route
	var stateErr string
	switch len(eligible) {
	case 0:
		stateErr = "no admitted EndpointCheck"
	case 1:
		check := eligible[0]
		ref := check.Spec.ForProvider.InferenceCredentialsSecretRef
		secret := &corev1.Secret{}
		if err := r.kube.Get(ctx, types.NamespacedName{Namespace: r.namespace, Name: ref.Name}, secret); err != nil {
			stateErr = "cannot read inference credential"
			break
		}
		token := secret.Data[ref.Key]
		if len(token) == 0 || len(token) > 4096 || bytes.ContainsAny(token, "\r\n") {
			stateErr = "inference credential is absent or malformed"
			break
		}
		next = &route{
			namespace: check.Namespace, checkName: check.Name,
			endpointID:      check.Status.AtProvider.EndpointID,
			expectedModelID: check.Spec.ForProvider.ExpectedModelID,
			token:           append([]byte(nil), token...),
		}
	default:
		stateErr = "multiple admitted EndpointChecks; single-route policy failed closed"
	}
	r.replace(next, stateErr)

	// Route state has already been replaced, so acknowledging finalizers now
	// guarantees that no deleting check remains routable.
	for _, check := range deleting {
		if removeString(&check.Finalizers, RouterDrainFinalizer) {
			if err := r.kube.Update(ctx, check); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
	}
	if next == nil {
		return errors.New(stateErr)
	}
	return nil
}

func (r *Router) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		_ = r.Refresh(ctx)
		select {
		case <-ctx.Done():
			r.withdraw("router stopped")
			return
		case <-t.C:
		}
	}
}

func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.URL.RawQuery != "" || req.Header.Get("Authorization") != "" || req.Header.Get("X-Api-Key") != "" {
		writeError(w, http.StatusBadRequest, "client credentials and query parameters are not accepted")
		return
	}
	if req.Method == http.MethodGet && req.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
		return
	}
	if err := r.Refresh(req.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, r.errorState())
		return
	}
	active := r.snapshot()
	if active == nil {
		writeError(w, http.StatusServiceUnavailable, r.errorState())
		return
	}
	defer clear(active.token)
	if req.Method == http.MethodGet && (req.URL.Path == "/readyz" || req.URL.Path == "/v1/models") {
		if req.URL.Path == "/readyz" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"status":"ready","model":"`+LogicalModelID+`"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": []any{map[string]any{
			"id": LogicalModelID, "object": "model", "owned_by": "provider-runpod",
		}}})
		return
	}
	if req.Method != http.MethodPost || req.URL.Path != "/v1/chat/completions" {
		writeError(w, http.StatusNotFound, "unsupported OpenAI operation")
		return
	}
	r.proxyChat(w, req, active)
}

func (r *Router) proxyChat(w http.ResponseWriter, req *http.Request, active *route) {
	body, err := io.ReadAll(io.LimitReader(req.Body, maxRequestBytes+1))
	if err != nil || len(body) > maxRequestBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "request body is invalid or too large")
		return
	}
	if secretPattern.Match(body) {
		writeError(w, http.StatusBadRequest, "request contains credential-like material")
		return
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil || containsSecretKey(payload) {
		writeError(w, http.StatusBadRequest, "request must be JSON and contain no secret fields")
		return
	}
	model, ok := payload["model"].(string)
	if !ok || model != LogicalModelID {
		writeError(w, http.StatusBadRequest, "model must be "+LogicalModelID)
		return
	}
	payload["model"] = active.expectedModelID
	body, _ = json.Marshal(payload)
	upstreamURL := strings.TrimRight(r.upstreamBase.String(), "/") + "/v2/" + url.PathEscape(active.endpointID) + "/openai/v1/chat/completions"
	upstream, err := http.NewRequestWithContext(req.Context(), http.MethodPost, upstreamURL, bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusBadGateway, "cannot construct upstream request")
		return
	}
	upstream.Header.Set("Authorization", "Bearer "+string(active.token))
	upstream.Header.Set("Accept", "application/json, text/event-stream")
	upstream.Header.Set("Content-Type", "application/json")
	resp, err := r.httpClient.Do(upstream)
	if err != nil {
		writeError(w, http.StatusBadGateway, "inference endpoint unavailable")
		return
	}
	defer resp.Body.Close() //nolint:errcheck
	for _, h := range []string{"Content-Type", "Cache-Control", "X-Request-Id"} {
		if value := resp.Header.Get(h); value != "" {
			w.Header().Set(h, value)
		}
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, 32<<20))
}

func (r *Router) replace(next *route, stateErr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active != nil {
		clear(r.active.token)
	}
	r.active = next
	r.stateError = stateErr
}

func (r *Router) withdraw(reason string) { r.replace(nil, reason) }

func (r *Router) snapshot() *route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.active == nil {
		return nil
	}
	copy := *r.active
	copy.token = append([]byte(nil), r.active.token...)
	return &copy
}

func (r *Router) errorState() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stateError
}

func containsSecretKey(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "password") || strings.Contains(lower, "secret") ||
				strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") || lower == "token" {
				return true
			}
			if containsSecretKey(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsSecretKey(child) {
				return true
			}
		}
	}
	return false
}

func removeString(values *[]string, target string) bool {
	for i, value := range *values {
		if value == target {
			*values = append((*values)[:i], (*values)[i+1:]...)
			return true
		}
	}
	return false
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"message": message, "type": fmt.Sprintf("router_http_%d", status)}})
}
