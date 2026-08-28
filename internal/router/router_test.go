// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package router

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
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
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"runpod-experiment","messages":[{"role":"user","content":"hello"}]}`))
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if response.Code != http.StatusOK || upstreamModel != "org/real-model" || upstreamAuth != "Bearer endpoint-token" {
		t.Fatalf("code=%d model=%q auth=%q", response.Code, upstreamModel, upstreamAuth)
	}
}

func TestCollisionAndInboundSecretsFailClosed(t *testing.T) {
	scheme := testScheme(t)
	one, two := readyCheck("one"), readyCheck("two")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(one, two, inferenceSecret()).Build()
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

func TestCredentialMaterialIsRejectedBeforeProxying(t *testing.T) {
	r := readyRouter(t, "http://127.0.0.1:1", readyCheck("one"))
	cases := map[string]string{
		"RunPod key":        `{"model":"runpod-experiment","messages":[{"role":"user","content":"rpa_abcdefghijk"}]}`,
		"GitHub token":      `{"model":"runpod-experiment","messages":[{"role":"user","content":"github_pat_abcdefghijklmnopqrstuvwxyz"}]}`,
		"AWS access key":    `{"model":"runpod-experiment","messages":[{"role":"user","content":"AKIA1234567890ABCDEF"}]}`,
		"private key":       `{"model":"runpod-experiment","messages":[{"role":"user","content":"-----BEGIN PRIVATE KEY-----"}]}`,
		"JWT":               `{"model":"runpod-experiment","messages":[{"role":"user","content":"eyJabcdefghijk.abcdefghijk.abcdefghijk"}]}`,
		"structured secret": `{"model":"runpod-experiment","metadata":{"credentials":"must-not-leave"}}`,
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
	body := `{"model":"runpod-experiment","messages":[{"role":"user","content":"hello"}]}`
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

func TestExpiredCheckAndProxyTransportFailClosed(t *testing.T) {
	check := readyCheck("expired")
	check.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	r := readyRouter(t, "http://127.0.0.1:1", check)
	if err := r.Refresh(context.Background()); err == nil {
		t.Fatal("expired check routed")
	}
	transport, ok := r.httpClient.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil {
		t.Fatal("router transport does not honor proxy environment")
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

func TestManagementPurposeCredentialFailsClosed(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("wrong-purpose")
	secret := inferenceSecret()
	secret.Labels[credentials.PurposeLabel] = credentials.PurposeManagement
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check, secret).Build()
	r, err := New(kube, "runpod-system")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err == nil || r.snapshot() != nil {
		t.Fatal("management-purpose credential was routed")
	}
}

func TestRefreshUsesDirectReaderForInferenceSecret(t *testing.T) {
	scheme := testScheme(t)
	check := readyCheck("direct-secret")
	cached := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(inferenceSecret()).Build()
	r, err := New(cached, "runpod-system", WithAPIReader(direct))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Refresh(context.Background()); err != nil || r.snapshot() == nil {
		t.Fatalf("direct Secret reader was not used: route=%v err=%v", r.snapshot(), err)
	}
}

func readyRouter(t *testing.T, upstream string, checks ...*verificationv1alpha1.EndpointCheck) *Router {
	t.Helper()
	scheme := testScheme(t)
	objects := make([]runtime.Object, 0, len(checks)+1)
	for _, c := range checks {
		objects = append(objects, c)
	}
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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "runpod-system", CreationTimestamp: metav1.Now()},
		Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{
			MaxLifetimeSeconds: 3600, EndpointID: "endpoint_1", ExpectedModelID: "org/real-model",
			InferenceCredentialsSecretRef: xpv2.LocalSecretKeySelector{LocalSecretReference: xpv2.LocalSecretReference{Name: "inference"}, Key: "token"},
		}},
		Status: verificationv1alpha1.EndpointCheckStatus{AtProvider: verificationv1alpha1.EndpointCheckObservation{
			EndpointID: "endpoint_1", Healthy: true, ModelVerified: true, ToolCallVerified: true,
			CredentialsSecretResourceVersion: "1", LastCheckedAt: metav1.Now(),
		}},
	}
	check.Status.ObservedGeneration = check.Generation
	check.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	return check
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
	return s
}
