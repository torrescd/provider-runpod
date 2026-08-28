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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/credentials"
	"github.com/torrescd/provider-runpod/internal/identifier"
)

const (
	LogicalModelID       = "runpod-experiment"
	RouterDrainFinalizer = "router.runpod.crossplane.io/drain"
	maxRequestBytes      = 256 << 10
	maxResponseBytes     = 32 << 20
	maxLivenessAge       = 90 * time.Second
)

var errResponseLimit = errors.New("upstream response exceeded limit")

var safeUpstreamRequestID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// credentialPattern catches common high-signal credentials before a request can
// leave the cluster. It is intentionally paired with structured key-name
// rejection below; neither mechanism attempts to transform or log the value.
var credentialPattern = regexp.MustCompile(`(?i)(bearer[[:space:]]+[a-z0-9._~+/=-]{8,}|rpa_[a-z0-9_-]{8,}|github_pat_[a-z0-9_]{20,}|gh[pousr]_[a-z0-9_]{20,}|glpat-[a-z0-9_-]{20,}|akia[a-z0-9]{16}|-----begin (rsa |ec |openssh )?private key-----|eyj[a-z0-9_-]{10,}\.[a-z0-9_-]{10,}\.[a-z0-9_-]{10,})`)

type route struct {
	namespace       string
	checkName       string
	endpointID      string
	expectedModelID string
	token           []byte
	ctx             context.Context
	cancel          context.CancelFunc
	inflight        sync.WaitGroup
}

type Option func(*Router) error

type Router struct {
	kube           client.Client
	reader         client.Reader
	namespace      string
	upstreamBase   *url.URL
	httpClient     *http.Client
	responseLimit  int64
	inferenceSlots chan struct{}
	refreshMu      sync.Mutex
	mu             sync.RWMutex
	active         *route
	stateError     string
}

func New(kube client.Client, namespace string, opts ...Option) (*Router, error) {
	if kube == nil || namespace == "" {
		return nil, errors.New("Kubernetes client and namespace are required")
	}
	u, _ := url.Parse("https://api.runpod.ai")
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = http.ProxyFromEnvironment
	transport.ResponseHeaderTimeout = 10 * time.Minute
	transport.MaxResponseHeaderBytes = 64 << 10
	r := &Router{
		kube: kube, reader: kube, namespace: namespace, upstreamBase: u,
		httpClient:     &http.Client{Transport: transport, Timeout: 10 * time.Minute, CheckRedirect: rejectRedirect},
		responseLimit:  maxResponseBytes,
		inferenceSlots: make(chan struct{}, 1),
		stateError:     "no admitted EndpointCheck",
	}
	for _, o := range opts {
		if err := o(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func rejectRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// WithAPIReader configures direct, uncached reads for credential Secrets.
func WithAPIReader(reader client.Reader) Option {
	return func(r *Router) error {
		if reader == nil {
			return errors.New("Kubernetes API reader is required")
		}
		r.reader = reader
		return nil
	}
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
	// Refresh is called both by the polling loop and synchronously by every
	// request. Serialize the full Kubernetes snapshot/read/commit transaction so
	// an older eligible snapshot can never commit after a newer withdrawal.
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()

	checks := &verificationv1alpha1.EndpointCheckList{}
	if err := r.reader.List(ctx, checks, client.InNamespace(r.namespace)); err != nil {
		r.withdraw("cannot read EndpointChecks")
		return err
	}
	revision, err := endpointCheckRevision(checks)
	if err != nil {
		r.withdraw("cannot validate EndpointCheck snapshot")
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
		verificationInterval := time.Duration(check.Spec.ForProvider.VerificationIntervalSeconds) * time.Second
		if verificationInterval == 0 {
			verificationInterval = time.Hour
		}
		fresh := check.Status.ObservedGeneration == check.Generation &&
			!observed.LastCheckedAt.IsZero() && !observed.LastCheckedAt.After(now.Add(30*time.Second)) &&
			now.Sub(observed.LastCheckedAt.Time) <= maxLivenessAge &&
			!observed.LastVerifiedAt.IsZero() && !observed.LastVerifiedAt.After(now.Add(30*time.Second)) &&
			now.Sub(observed.LastVerifiedAt.Time) <= verificationInterval+maxLivenessAge
		if ready && hasString(check.Finalizers, RouterDrainFinalizer) && fresh && now.Before(expiresAt) && observed.Healthy && observed.ModelVerified &&
			observed.ToolCallVerified && observed.EndpointID != "" && r.endpointRevisionMatches(ctx, check, now) {
			if identifier.ValidateRunPodID(observed.EndpointID) != nil {
				continue
			}
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
		if err := r.reader.Get(ctx, types.NamespacedName{Namespace: r.namespace, Name: ref.Name}, secret); err != nil {
			stateErr = "cannot read inference credential"
			break
		}
		if err := credentials.RequirePurpose(secret, credentials.PurposeInference); err != nil {
			stateErr = "inference credential has the wrong purpose"
			break
		}
		if check.Status.AtProvider.CredentialsSecretResourceVersion == "" ||
			secret.ResourceVersion != check.Status.AtProvider.CredentialsSecretResourceVersion {
			stateErr = "inference credential changed since verification"
			break
		}
		token := secret.Data[ref.Key]
		if len(token) == 0 || len(token) > 4096 || string(token) != strings.TrimSpace(string(token)) || bytes.ContainsAny(token, "\r\n") {
			stateErr = "inference credential is absent or malformed"
			break
		}
		if !r.endpointRevisionMatches(ctx, check, time.Now()) {
			stateErr = "referenced Endpoint changed since verification"
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
	// A direct second read closes the window in which a deletion, admission
	// withdrawal, credential rotation, or new collision could occur while the
	// Secret was being read. The drain finalizer is acknowledged only from a
	// snapshot proven unchanged through this point.
	current := &verificationv1alpha1.EndpointCheckList{}
	if err := r.reader.List(ctx, current, client.InNamespace(r.namespace)); err != nil {
		r.withdraw("cannot revalidate EndpointChecks")
		return err
	}
	currentRevision, err := endpointCheckRevision(current)
	if err != nil {
		r.withdraw("cannot validate EndpointCheck snapshot")
		return err
	}
	if currentRevision != revision {
		r.withdraw("EndpointChecks changed during route refresh")
		return errors.New("EndpointChecks changed during route refresh")
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

func (r *Router) endpointRevisionMatches(ctx context.Context, check *verificationv1alpha1.EndpointCheck, now time.Time) bool {
	ref := check.Spec.ForProvider.EndpointIDRef
	observed := check.Status.AtProvider
	if ref == nil {
		return false
	}
	ep := &serverlessv1alpha1.Endpoint{}
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: check.Namespace, Name: ref.Name}, ep); err != nil {
		return false
	}
	templateRef := ep.Spec.ForProvider.TemplateIDRef
	if templateRef == nil {
		return false
	}
	template := &serverlessv1alpha1.Template{}
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: ep.Namespace, Name: templateRef.Name}, template); err != nil {
		return false
	}
	expiresAt := ep.CreationTimestamp.Add(time.Duration(ep.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	templateExpiresAt := template.CreationTimestamp.Add(time.Duration(template.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	endpointID := meta.GetExternalName(ep)
	templateID := meta.GetExternalName(template)
	return ep.DeletionTimestamp.IsZero() && ep.Status.ObservedGeneration == ep.Generation &&
		ep.Status.GetCondition(xpv2.TypeReady).Status == corev1.ConditionTrue &&
		ep.Status.GetCondition(xpv2.TypeSynced).Status == corev1.ConditionTrue &&
		now.Before(expiresAt) && ep.Status.AtProvider.FlashBootEvidenceCurrent(now) && ep.Status.AtProvider.WorkerSecurityEvidenceCurrent() &&
		template.DeletionTimestamp.IsZero() && template.Status.ObservedGeneration == template.Generation &&
		template.Status.GetCondition(xpv2.TypeReady).Status == corev1.ConditionTrue &&
		template.Status.GetCondition(xpv2.TypeSynced).Status == corev1.ConditionTrue &&
		(template.CreationTimestamp.IsZero() || now.Before(templateExpiresAt)) &&
		string(template.UID) == ep.Status.AtProvider.TemplateResourceUID &&
		template.Generation == ep.Status.AtProvider.TemplateResourceGeneration &&
		template.Spec.ForProvider.ImageName == ep.Status.AtProvider.TemplateImageDigest &&
		identifier.ValidateRunPodID(endpointID) == nil && endpointID == ep.Status.AtProvider.ID && endpointID == observed.EndpointID &&
		identifier.ValidateRunPodID(templateID) == nil && templateID == template.Status.AtProvider.ID && templateID == ep.Status.AtProvider.TemplateID &&
		string(ep.UID) == observed.EndpointResourceUID && ep.Generation == observed.EndpointResourceGeneration &&
		ep.Status.AtProvider.Version != nil && observed.EndpointVersion != nil &&
		*ep.Status.AtProvider.Version == *observed.EndpointVersion &&
		ep.Status.AtProvider.TemplateResourceUID == observed.TemplateResourceUID &&
		ep.Status.AtProvider.TemplateResourceGeneration == observed.TemplateResourceGeneration &&
		ep.Status.AtProvider.TemplateImageDigest == observed.TemplateImageDigest
}

func endpointCheckRevision(checks *verificationv1alpha1.EndpointCheckList) (string, error) {
	items := append([]verificationv1alpha1.EndpointCheck(nil), checks.Items...)
	sort.Slice(items, func(i, j int) bool {
		if items[i].Namespace == items[j].Namespace {
			return items[i].Name < items[j].Name
		}
		return items[i].Namespace < items[j].Namespace
	})
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", fmt.Errorf("encode EndpointCheck revision: %w", err)
	}
	return string(encoded), nil
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
	active, release := r.acquire()
	if active == nil {
		writeError(w, http.StatusServiceUnavailable, r.errorState())
		return
	}
	defer release()
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
	if credentialPattern.Match(body) {
		writeError(w, http.StatusBadRequest, "request contains credential-like material")
		return
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil || containsSecretKey(payload) {
		writeError(w, http.StatusBadRequest, "request must be JSON and contain no secret fields")
		return
	}
	if err := validateChatPayload(payload); err != nil {
		writeError(w, http.StatusBadRequest, "request is outside the bounded OpenAI chat schema")
		return
	}
	canonical, err := json.Marshal(payload)
	if err != nil || credentialPattern.Match(canonical) {
		writeError(w, http.StatusBadRequest, "request contains credential-like material")
		return
	}
	model, ok := payload["model"].(string)
	if !ok || model != LogicalModelID {
		writeError(w, http.StatusBadRequest, "model must be "+LogicalModelID)
		return
	}
	payload["model"] = active.expectedModelID
	body, _ = json.Marshal(payload)
	if !r.acquireInferenceSlot() {
		writeError(w, http.StatusTooManyRequests, "model-router concurrency limit reached")
		return
	}
	defer r.releaseInferenceSlot()
	upstreamURL := strings.TrimRight(r.upstreamBase.String(), "/") + "/v2/" + url.PathEscape(active.endpointID) + "/openai/v1/chat/completions"
	upstreamContext, cancelUpstream := context.WithCancel(req.Context())
	stopRouteCancellation := context.AfterFunc(active.ctx, cancelUpstream)
	defer func() {
		stopRouteCancellation()
		cancelUpstream()
	}()
	upstream, err := http.NewRequestWithContext(upstreamContext, http.MethodPost, upstreamURL, bytes.NewReader(body))
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
	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	// Prompt and model output must never inherit an upstream cacheable policy.
	w.Header().Set("Cache-Control", "no-store")
	if requestID := resp.Header.Get("X-Request-Id"); safeUpstreamRequestID.MatchString(requestID) {
		w.Header().Set("X-Request-Id", requestID)
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Add("Trailer", "X-Provider-Runpod-Error")
	w.WriteHeader(resp.StatusCode)
	if err := copyBoundedAndFlush(w, resp.Body, r.responseLimit); err != nil {
		failure := "upstream-read-failed"
		if errors.Is(err, errResponseLimit) {
			failure = "response-limit-exceeded"
		}
		// The status/body may already be streaming, so a predeclared HTTP trailer
		// is the only protocol-safe explicit terminal failure signal.
		w.Header().Set("X-Provider-Runpod-Error", failure)
	}
}

func copyBoundedAndFlush(dst http.ResponseWriter, src io.Reader, limit int64) error {
	if limit <= 0 {
		return errResponseLimit
	}
	buf := make([]byte, 32*1024)
	remaining := limit
	for {
		if remaining == 0 {
			var probe [1]byte
			n, err := src.Read(probe[:])
			if n > 0 {
				return errResponseLimit
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			continue
		}
		readBuffer := buf
		if remaining < int64(len(readBuffer)) {
			readBuffer = readBuffer[:remaining]
		}
		n, err := src.Read(readBuffer)
		if n > 0 {
			written, writeErr := dst.Write(readBuffer[:n])
			remaining -= int64(written)
			if writeErr != nil {
				return writeErr
			}
			if written != n {
				return io.ErrShortWrite
			}
			if flusher, ok := dst.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (r *Router) replace(next *route, stateErr string) {
	if next != nil && next.cancel == nil {
		next.ctx, next.cancel = context.WithCancel(context.Background())
	}
	r.mu.Lock()
	previous := r.active
	if previous == next {
		r.stateError = stateErr
		r.mu.Unlock()
		return
	}
	if routesEquivalent(previous, next) {
		// Refresh runs every two seconds and before every request. Retain the
		// existing route lease/cancellation domain when the admitted target and
		// credential are unchanged, otherwise each refresh would cancel all
		// in-flight streams merely because it allocated a new route struct.
		r.stateError = stateErr
		r.mu.Unlock()
		clear(next.token)
		next.cancel()
		return
	}
	r.active = next
	r.stateError = stateErr
	if previous != nil && previous != next {
		previous.cancel()
	}
	r.mu.Unlock()

	// No new lease can be acquired after active is swapped. Wait for every
	// request that acquired the old route to observe cancellation and exit
	// before erasing its token or acknowledging a drain finalizer.
	if previous != nil && previous != next {
		previous.inflight.Wait()
		clear(previous.token)
	}
}

func routesEquivalent(a, b *route) bool {
	return a != nil && b != nil && a.namespace == b.namespace && a.checkName == b.checkName &&
		a.endpointID == b.endpointID && a.expectedModelID == b.expectedModelID && bytes.Equal(a.token, b.token)
}

func (r *Router) withdraw(reason string) { r.replace(nil, reason) }

func (r *Router) snapshot() *route {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.active == nil {
		return nil
	}
	return &route{
		namespace: r.active.namespace, checkName: r.active.checkName,
		endpointID: r.active.endpointID, expectedModelID: r.active.expectedModelID,
		token: append([]byte(nil), r.active.token...),
	}
}

func (r *Router) acquire() (*route, func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active == nil {
		return nil, func() {}
	}
	active := r.active
	active.inflight.Add(1)
	return active, active.inflight.Done
}

func (r *Router) errorState() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stateError
}

func (r *Router) acquireInferenceSlot() bool {
	select {
	case r.inferenceSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (r *Router) releaseInferenceSlot() { <-r.inferenceSlots }

func containsSecretKey(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			lower := strings.ToLower(key)
			if strings.Contains(lower, "password") || strings.Contains(lower, "secret") ||
				strings.Contains(lower, "api_key") || strings.Contains(lower, "apikey") ||
				lower == "token" || lower == "auth" || lower == "authorization" ||
				lower == "credential" || lower == "credentials" ||
				strings.Contains(lower, "private_key") || strings.Contains(lower, "client_key") {
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

func hasString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
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
