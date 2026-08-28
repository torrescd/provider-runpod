// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpointcheck

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/credentials"
	"github.com/torrescd/provider-runpod/internal/inference"
)

type fakeVerifier struct {
	result      inference.Result
	err         error
	healthErr   error
	verifyCalls *int
	healthCalls *int
}

func (f fakeVerifier) Verify(context.Context, string) (inference.Result, error) {
	if f.verifyCalls != nil {
		*f.verifyCalls++
	}
	return f.result, f.err
}

func (f fakeVerifier) CheckHealth(context.Context) error {
	if f.healthCalls != nil {
		*f.healthCalls++
	}
	return f.healthErr
}

func TestReconcilePublishesReadyOnlyAfterAllChecks(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{result: inference.Result{Healthy: true, ModelVerified: true, ToolCallVerified: true}}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	secret := inferenceSecretObject(credentials.PurposeInference)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret, readyEndpoint(check, "endpoint_1"), readyTemplate(check)).Build()
	r := &reconciler{kube: kube, reader: kube}
	_, err := r.Reconcile(context.Background(), request(check))
	if err != nil {
		t.Fatal(err)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue || !got.Status.AtProvider.ToolCallVerified || got.Status.AtProvider.EndpointID != "endpoint_1" || got.Status.ObservedGeneration != got.Generation || got.Status.AtProvider.CredentialsSecretResourceVersion == "" || got.Status.AtProvider.LastVerificationAttemptAt.IsZero() {
		t.Fatalf("unexpected status: %+v", got.Status)
	}
}

func TestSteadyStateLivenessDoesNotRepeatBillableVerification(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifyCalls := 0
	healthCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{verifyCalls: &verifyCalls, healthCalls: &healthCalls, result: inference.Result{Healthy: true, ModelVerified: true, ToolCallVerified: true}}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	secret := inferenceSecretObject(credentials.PurposeInference)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret, readyEndpoint(check, "endpoint_1"), readyTemplate(check)).Build()
	now := check.CreationTimestamp.Time.Add(time.Second)
	r := &reconciler{kube: kube, reader: kube, now: func() time.Time { return now }}

	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		now = now.Add(pollInterval)
		if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
			t.Fatal(err)
		}
	}
	if verifyCalls != 1 {
		t.Fatalf("full verification calls=%d over five minutes, want one admission probe", verifyCalls)
	}
	if healthCalls != 9 {
		t.Fatalf("cheap health calls=%d before the five-minute FlashBoot proof refresh, want nine", healthCalls)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.AtProvider.LastCheckedAt.IsZero() || got.Status.AtProvider.LastVerifiedAt.IsZero() ||
		!got.Status.AtProvider.LastCheckedAt.After(got.Status.AtProvider.LastVerifiedAt.Time) {
		t.Fatalf("cheap liveness and expensive verification timestamps not separated: %+v", got.Status.AtProvider)
	}
}

func TestWorkerAdmissionWaitsForPostVerificationObservation(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifyCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{verifyCalls: &verifyCalls, result: inference.Result{Healthy: true, ModelVerified: true, ToolCallVerified: true}}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	check.Spec.ForProvider.MaxLifetimeSeconds = 7200
	endpoint := readyEndpoint(check, "endpoint_1")
	endpoint.Spec.ForProvider.MaxLifetimeSeconds = 7200
	endpoint.Status.AtProvider.WorkerSecurityValidated = false
	endpoint.Status.AtProvider.WorkerSecurityProofVersion = nil
	endpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.Time{}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check, endpoint).WithObjects(
		check, endpoint, readyTemplate(check), inferenceSecretObject(credentials.PurposeInference),
	).Build()
	now := check.CreationTimestamp.Add(time.Second)
	r := &reconciler{kube: kube, reader: kube, now: func() time.Time { return now }}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	gotCheck := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, gotCheck); err != nil {
		t.Fatal(err)
	}
	if gotCheck.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionFalse || verifyCalls != 1 {
		t.Fatalf("unobserved cold worker was admitted: ready=%s verifies=%d", gotCheck.Status.GetCondition(xpv2.TypeReady).Status, verifyCalls)
	}
	gotEndpoint := &serverlessv1alpha1.Endpoint{}
	endpointKey := types.NamespacedName{Namespace: endpoint.Namespace, Name: endpoint.Name}
	if err := kube.Get(context.Background(), endpointKey, gotEndpoint); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	gotEndpoint.Status.AtProvider.WorkerSecurityValidated = true
	gotEndpoint.Status.AtProvider.WorkerSecurityProofVersion = int32Ptr(1)
	gotEndpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.NewTime(now)
	if err := kube.Status().Update(context.Background(), gotEndpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), request(check).NamespacedName, gotCheck); err != nil {
		t.Fatal(err)
	}
	if gotCheck.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue || verifyCalls != 1 {
		t.Fatalf("post-verification worker proof did not admit exactly once: ready=%s verifies=%d", gotCheck.Status.GetCondition(xpv2.TypeReady).Status, verifyCalls)
	}
}

func TestMissingWorkerWithdrawsCheckUntilNewWorkerObservation(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifyCalls := 0
	healthCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{verifyCalls: &verifyCalls, healthCalls: &healthCalls, result: inference.Result{Healthy: true, ModelVerified: true, ToolCallVerified: true}}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	check.Spec.ForProvider.MaxLifetimeSeconds = 7200
	endpoint := readyEndpoint(check, "endpoint_1")
	endpoint.Spec.ForProvider.MaxLifetimeSeconds = 7200
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check, endpoint).WithObjects(
		check, endpoint, readyTemplate(check), inferenceSecretObject(credentials.PurposeInference),
	).Build()
	now := check.CreationTimestamp.Add(time.Second)
	r := &reconciler{kube: kube, reader: kube, now: func() time.Time { return now }}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}

	endpointKey := types.NamespacedName{Namespace: endpoint.Namespace, Name: endpoint.Name}
	gotEndpoint := &serverlessv1alpha1.Endpoint{}
	if err := kube.Get(context.Background(), endpointKey, gotEndpoint); err != nil {
		t.Fatal(err)
	}
	gotEndpoint.Status.AtProvider.WorkerSecurityValidated = false
	gotEndpoint.Status.AtProvider.WorkerSecurityProofVersion = nil
	gotEndpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.Time{}
	if err := kube.Status().Update(context.Background(), gotEndpoint); err != nil {
		t.Fatal(err)
	}
	now = now.Add(pollInterval)
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	gotCheck := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, gotCheck); err != nil {
		t.Fatal(err)
	}
	if gotCheck.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionFalse || verifyCalls != 1 || healthCalls != 1 {
		t.Fatalf("empty-worker state was not withdrawn cheaply: ready=%s verifies=%d health=%d", gotCheck.Status.GetCondition(xpv2.TypeReady).Status, verifyCalls, healthCalls)
	}

	// Simulate a new cold worker being directly observed after the original
	// verification. The existing model/tool proof can be reused for the same
	// rollout only after this fresh placement/cost/SecureCloud proof appears.
	now = now.Add(time.Second)
	if err := kube.Get(context.Background(), endpointKey, gotEndpoint); err != nil {
		t.Fatal(err)
	}
	gotEndpoint.Status.AtProvider.WorkerSecurityValidated = true
	gotEndpoint.Status.AtProvider.WorkerSecurityProofVersion = int32Ptr(1)
	gotEndpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.NewTime(now)
	if err := kube.Status().Update(context.Background(), gotEndpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), request(check).NamespacedName, gotCheck); err != nil {
		t.Fatal(err)
	}
	if gotCheck.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue || verifyCalls != 1 {
		t.Fatalf("fresh cold-worker observation did not restore admission: ready=%s verifies=%d", gotCheck.Status.GetCondition(xpv2.TypeReady).Status, verifyCalls)
	}
}

func TestMissedWorkerHandshakeRetriesOnlyAfterVerificationInterval(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifyCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{verifyCalls: &verifyCalls, result: inference.Result{Healthy: true, ModelVerified: true, ToolCallVerified: true}}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	check.Spec.ForProvider.MaxLifetimeSeconds = 7200
	endpoint := readyEndpoint(check, "endpoint_1")
	endpoint.Spec.ForProvider.MaxLifetimeSeconds = 7200
	endpoint.Status.AtProvider.WorkerSecurityValidated = false
	endpoint.Status.AtProvider.WorkerSecurityProofVersion = nil
	endpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.Time{}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check, endpoint).WithObjects(
		check, endpoint, readyTemplate(check), inferenceSecretObject(credentials.PurposeInference),
	).Build()
	now := check.CreationTimestamp.Add(time.Second)
	r := &reconciler{kube: kube, reader: kube, now: func() time.Time { return now }}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	firstAttempt := now
	now = firstAttempt.Add(workerObservationHandshake + time.Second)
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	if verifyCalls != 1 {
		t.Fatalf("missed worker handshake retried early: verifies=%d", verifyCalls)
	}
	now = firstAttempt.Add(time.Duration(check.Spec.ForProvider.VerificationIntervalSeconds) * time.Second)
	gotEndpoint := &serverlessv1alpha1.Endpoint{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: endpoint.Namespace, Name: endpoint.Name}, gotEndpoint); err != nil {
		t.Fatal(err)
	}
	gotEndpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.NewTime(now)
	if err := kube.Status().Update(context.Background(), gotEndpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	if verifyCalls != 2 {
		t.Fatalf("missed worker handshake did not retry at explicit interval: verifies=%d", verifyCalls)
	}
}

func TestFailedVerificationIsNotRetriedBeforeCostInterval(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifyCalls := 0
	healthCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{
			verifyCalls: &verifyCalls,
			healthCalls: &healthCalls,
			err:         context.DeadlineExceeded,
		}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	check.Spec.ForProvider.MaxLifetimeSeconds = 7200
	secret := inferenceSecretObject(credentials.PurposeInference)
	endpoint := readyEndpoint(check, "endpoint_1")
	endpoint.Spec.ForProvider.MaxLifetimeSeconds = 7200
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret, endpoint, readyTemplate(check)).Build()
	now := check.CreationTimestamp.Time.Add(time.Second)
	r := &reconciler{kube: kube, reader: kube, now: func() time.Time { return now }}

	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	afterFirst := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, afterFirst); err != nil {
		t.Fatal(err)
	}
	firstAttempt := afterFirst.Status.AtProvider.LastVerificationAttemptAt
	for i := 0; i < 9; i++ {
		now = now.Add(pollInterval)
		if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
			t.Fatal(err)
		}
	}
	if verifyCalls != 1 {
		t.Fatalf("failed or timed-out verification calls=%d before retry interval, want one", verifyCalls)
	}
	if healthCalls != 9 {
		t.Fatalf("cheap health calls=%d before the five-minute FlashBoot proof refresh, want nine", healthCalls)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionFalse {
		t.Fatalf("failed full verification became Ready during retry backoff: %+v", got.Status)
	}
	if got.Status.AtProvider.LastVerificationAttemptAt.IsZero() ||
		!got.Status.AtProvider.LastVerificationAttemptAt.Equal(&firstAttempt) ||
		!got.Status.AtProvider.LastVerifiedAt.IsZero() {
		t.Fatalf("unexpected attempt/success timestamps after failed verification: %+v", got.Status.AtProvider)
	}

	// The expensive probe is eligible exactly once the configured interval has
	// elapsed, even when the previous attempt ended in a request timeout. The
	// managed Endpoint controller independently refreshes its five-minute
	// FlashBoot=false proof, so model that successful reassertion here.
	now = firstAttempt.Add(time.Duration(check.Spec.ForProvider.VerificationIntervalSeconds) * time.Second)
	endpoint = &serverlessv1alpha1.Endpoint{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: check.Namespace, Name: check.Spec.ForProvider.EndpointIDRef.Name}, endpoint); err != nil {
		t.Fatal(err)
	}
	endpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.NewTime(now)
	if err := kube.Update(context.Background(), endpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	if verifyCalls != 2 {
		t.Fatalf("verification calls=%d at retry interval, want two total", verifyCalls)
	}
}

func TestNonInferenceSecretIsRejected(t *testing.T) {
	scheme := endpointCheckScheme(t)
	check := checkObject()
	secret := inferenceSecretObject(credentials.PurposeManagement)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret, readyEndpoint(check, "endpoint_1"), readyTemplate(check)).Build()
	r := &reconciler{kube: kube, reader: kube}
	_, err := r.Reconcile(context.Background(), request(check))
	if err != nil {
		t.Fatal(err)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	_ = kube.Get(context.Background(), request(check).NamespacedName, got)
	if got.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionFalse {
		t.Fatal("non-inference Secret was admitted")
	}
}

func TestReconcileDirectReadsSecretAndEndpoint(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	newVerifier = func(endpointID string, token []byte, _ time.Duration) (verifier, error) {
		if endpointID != "endpoint_direct" || string(token) != "endpoint-only" {
			t.Fatalf("unexpected verifier input endpoint=%q token=%q", endpointID, token)
		}
		return fakeVerifier{result: inference.Result{Healthy: true, ModelVerified: true, ToolCallVerified: true}}, nil
	}

	scheme := endpointCheckScheme(t)
	check := checkObject()
	endpoint := readyEndpoint(check, "endpoint_direct")
	meta.SetExternalName(endpoint, "endpoint_direct")
	endpoint.Status.AtProvider.ID = "endpoint_direct"

	// The cached client deliberately has no Secret or Endpoint. Reconciliation
	// can succeed only if both objects are read through the direct reader.
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check.DeepCopy(), endpoint, readyTemplate(check), inferenceSecretObject(credentials.PurposeInference)).Build()
	r := &reconciler{kube: kube, reader: direct}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue || got.Status.AtProvider.EndpointID != "endpoint_direct" {
		t.Fatalf("direct-read reconciliation was not Ready: %+v", got.Status)
	}
}

func TestReconcileUsesDirectCurrentEndpointCheckStatus(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifyCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{verifyCalls: &verifyCalls}, nil
	}
	scheme := endpointCheckScheme(t)
	stale := checkObject()
	current := stale.DeepCopy()
	now := current.CreationTimestamp.Time.Add(time.Minute)
	current.Status.ObservedGeneration = current.Generation
	current.Status.AtProvider = verificationv1alpha1.EndpointCheckObservation{
		EndpointID: "endpoint_1", EndpointResourceUID: "endpoint-uid", EndpointResourceGeneration: 1, EndpointVersion: int32Ptr(1),
		TemplateResourceUID: "template-uid", TemplateResourceGeneration: 1, TemplateImageDigest: testTemplateImage,
		Healthy: true, ModelVerified: true, ToolCallVerified: true,
		CredentialsSecretResourceVersion: "999",
		LastCheckedAt:                    metav1.NewTime(now), LastVerificationAttemptAt: metav1.NewTime(now), LastVerifiedAt: metav1.NewTime(now),
	}
	current.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	cached := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(stale).WithObjects(stale).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(current, readyEndpoint(current, "endpoint_1"), readyTemplate(current), inferenceSecretObject(credentials.PurposeInference)).Build()
	r := &reconciler{kube: cached, reader: direct, now: func() time.Time { return now.Add(time.Second) }}
	if _, err := r.Reconcile(context.Background(), request(stale)); err != nil {
		t.Fatal(err)
	}
	if verifyCalls != 0 {
		t.Fatal("stale cached EndpointCheck status repeated the billable verification")
	}
}

func TestEndpointRolloutRevisionForcesFullVerification(t *testing.T) {
	now := time.Now()
	check := checkObject()
	check.Status.ObservedGeneration = check.Generation
	check.Status.AtProvider = verificationv1alpha1.EndpointCheckObservation{
		EndpointID: "endpoint_1", EndpointResourceUID: "endpoint-uid",
		EndpointResourceGeneration: 3, EndpointVersion: int32Ptr(7),
		TemplateResourceUID: "template-uid", TemplateResourceGeneration: 1, TemplateImageDigest: testTemplateImage,
		Healthy: true, ModelVerified: true, ToolCallVerified: true,
		CredentialsSecretResourceVersion: "secret-rv",
		LastCheckedAt:                    metav1.NewTime(now), LastVerificationAttemptAt: metav1.NewTime(now), LastVerifiedAt: metav1.NewTime(now),
	}
	current := endpointBinding{id: "endpoint_1", resourceUID: "endpoint-uid", resourceGeneration: 3, version: 7, templateUID: "template-uid", templateGeneration: 1, templateImage: testTemplateImage}
	if fullVerificationDue(check, current, "secret-rv", now.Add(time.Minute)) {
		t.Fatal("unchanged rollout repeated full verification before the cost interval")
	}
	rolled := current
	rolled.version++
	if !fullVerificationDue(check, rolled, "secret-rv", now.Add(time.Minute)) {
		t.Fatal("RunPod Endpoint version change did not force full verification")
	}
	recreated := current
	recreated.resourceUID = "replacement-uid"
	if !fullVerificationDue(check, recreated, "secret-rv", now.Add(time.Minute)) {
		t.Fatal("referenced Endpoint replacement did not force full verification")
	}
}

func TestReferencedEndpointMustBeReadyForCurrentGeneration(t *testing.T) {
	scheme := endpointCheckScheme(t)
	check := checkObject()
	endpoint := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{
		Name: "endpoint", Namespace: check.Namespace, Generation: 2,
	}}
	meta.SetExternalName(endpoint, "endpoint_stale")
	endpoint.Status.ObservedGeneration = 1
	endpoint.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(endpoint).Build()
	r := &reconciler{reader: reader}
	if _, err := r.resolveEndpoint(context.Background(), check); err == nil {
		t.Fatal("Ready condition from a stale Endpoint generation was accepted")
	}
}

func TestReferencedEndpointRequiresCurrentFlashBootEvidence(t *testing.T) {
	scheme := endpointCheckScheme(t)
	check := checkObject()
	endpoint := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{
		Name: "endpoint", Namespace: check.Namespace, Generation: 1,
	}}
	meta.SetExternalName(endpoint, "endpoint_1")
	endpoint.Status.ObservedGeneration = 1
	endpoint.Status.AtProvider.ID = "endpoint_1"
	endpoint.Status.AtProvider.Version = int32Ptr(2)
	endpoint.Status.AtProvider.FlashBootDisabled = true
	endpoint.Status.AtProvider.FlashBootEvidenceVersion = int32Ptr(2)
	endpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.NewTime(time.Now().Add(-serverlessv1alpha1.FlashBootEvidenceMaxAge - time.Second))
	endpoint.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(endpoint).Build()
	if _, err := (&reconciler{reader: reader}).resolveEndpoint(context.Background(), check); err == nil {
		t.Fatal("expired FlashBoot write evidence was accepted")
	}
}

func TestExpiredReferencedEndpointDeletesCheckWithoutVerifierCall(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifierCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		verifierCalls++
		return fakeVerifier{}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	endpoint := readyEndpoint(check, "endpoint_1")
	endpoint.CreationTimestamp = metav1.NewTime(check.CreationTimestamp.Add(-2 * time.Hour))
	endpoint.Spec.ForProvider.MaxLifetimeSeconds = 3600
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, endpoint, readyTemplate(check), inferenceSecretObject(credentials.PurposeInference)).Build()
	now := check.CreationTimestamp.Add(time.Minute)
	r := &reconciler{kube: kube, reader: kube, now: func() time.Time { return now }}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	if verifierCalls != 0 {
		t.Fatalf("expired Endpoint reached inference verifier %d times", verifierCalls)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.DeletionTimestamp.IsZero() {
		t.Fatal("EndpointCheck remained live after its referenced Endpoint lifetime")
	}
}

func TestReferencedEndpointMustRemainBoundToCurrentTemplateRevision(t *testing.T) {
	scheme := endpointCheckScheme(t)
	check := checkObject()
	endpoint := readyEndpoint(check, "endpoint_1")
	template := readyTemplate(check)
	template.Generation = 2
	template.Status.ObservedGeneration = 2
	template.Spec.ForProvider.ImageName = "registry.example/model@sha256:" + strings.Repeat("b", 64)
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(endpoint, template).Build()
	if _, err := (&reconciler{reader: reader}).resolveEndpoint(context.Background(), check); err == nil {
		t.Fatal("Endpoint bound to the prior Template generation/digest was accepted")
	}
}

func TestExternalNameAnnotationTamperingNeverReachesInference(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	verifierCalls := 0
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		verifierCalls++
		return fakeVerifier{}, nil
	}
	for name, mutate := range map[string]func(*serverlessv1alpha1.Endpoint, *serverlessv1alpha1.Template){
		"Endpoint annotation": func(endpoint *serverlessv1alpha1.Endpoint, _ *serverlessv1alpha1.Template) {
			meta.SetExternalName(endpoint, "endpoint_tampered")
		},
		"Template annotation": func(_ *serverlessv1alpha1.Endpoint, template *serverlessv1alpha1.Template) {
			meta.SetExternalName(template, "template_tampered")
		},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := endpointCheckScheme(t)
			check := checkObject()
			endpoint := readyEndpoint(check, "endpoint_1")
			template := readyTemplate(check)
			mutate(endpoint, template)
			kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(
				check, endpoint, template, inferenceSecretObject(credentials.PurposeInference),
			).Build()
			r := &reconciler{kube: kube, reader: kube}
			if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
				t.Fatal(err)
			}
		})
	}
	if verifierCalls != 0 {
		t.Fatalf("annotation tampering reached inference verifier %d times", verifierCalls)
	}
}

func TestFailureConditionsNeverRetainInferenceCredential(t *testing.T) {
	original := newVerifier
	defer func() { newVerifier = original }()
	newVerifier = func(string, []byte, time.Duration) (verifier, error) {
		return fakeVerifier{err: errors.New("upstream rejected rpa_supersecretvalue")}, nil
	}
	scheme := endpointCheckScheme(t)
	check := checkObject()
	secret := inferenceSecretObject(credentials.PurposeInference)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret, readyEndpoint(check, "endpoint_1"), readyTemplate(check)).Build()
	r := &reconciler{kube: kube, reader: kube}
	if _, err := r.Reconcile(context.Background(), request(check)); err != nil {
		t.Fatal(err)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	for _, condition := range got.Status.Conditions {
		if strings.Contains(condition.Message, "rpa_supersecretvalue") {
			t.Fatalf("condition %q retained inference credential", condition.Type)
		}
	}
}

func inferenceSecretObject(purpose string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "inference", Namespace: "runpod-system",
			Labels: map[string]string{credentials.PurposeLabel: purpose},
		},
		Data: map[string][]byte{"token": []byte("endpoint-only")},
	}
}

func checkObject() *verificationv1alpha1.EndpointCheck {
	return &verificationv1alpha1.EndpointCheck{
		ObjectMeta: metav1.ObjectMeta{Name: "check", Namespace: "runpod-system", CreationTimestamp: metav1.Now(), Finalizers: []string{RouterDrainFinalizer}},
		Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{
			MaxLifetimeSeconds: 3600, EndpointIDRef: &xpv2.Reference{Name: "endpoint"}, ExpectedModelID: "org/model",
			VerificationIntervalSeconds: 3600, TimeoutSeconds: 180,
			InferenceCredentialsSecretRef: xpv2.LocalSecretKeySelector{LocalSecretReference: xpv2.LocalSecretReference{Name: "inference"}, Key: "token"},
		}},
	}
}

func readyEndpoint(check *verificationv1alpha1.EndpointCheck, id string) *serverlessv1alpha1.Endpoint {
	endpoint := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{
		Name: check.Spec.ForProvider.EndpointIDRef.Name, Namespace: check.Namespace, UID: "endpoint-uid", Generation: 1, CreationTimestamp: check.CreationTimestamp,
	}, Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{MaxLifetimeSeconds: 3600, TemplateIDRef: &xpv2.Reference{Name: "template"}}}}
	meta.SetExternalName(endpoint, id)
	endpoint.Status.ObservedGeneration = 1
	endpoint.Status.AtProvider.ID = id
	endpoint.Status.AtProvider.TemplateID = "template_1"
	endpoint.Status.AtProvider.Version = int32Ptr(1)
	endpoint.Status.AtProvider.TemplateResourceUID = "template-uid"
	endpoint.Status.AtProvider.TemplateResourceGeneration = 1
	endpoint.Status.AtProvider.TemplateImageDigest = testTemplateImage
	endpoint.Status.AtProvider.FlashBootDisabled = true
	endpoint.Status.AtProvider.FlashBootEvidenceVersion = int32Ptr(1)
	endpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.Now()
	endpoint.Status.AtProvider.WorkerSecurityValidated = true
	endpoint.Status.AtProvider.WorkerSecurityProofVersion = int32Ptr(1)
	endpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.Now()
	endpoint.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	return endpoint
}

const testTemplateImage = "registry.example/model@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func readyTemplate(check *verificationv1alpha1.EndpointCheck) *serverlessv1alpha1.Template {
	template := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: check.Namespace, UID: "template-uid", Generation: 1},
		Spec:       serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{ImageName: testTemplateImage}},
	}
	meta.SetExternalName(template, "template_1")
	template.Status.ObservedGeneration = 1
	template.Status.AtProvider.ID = "template_1"
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	return template
}

func endpointCheckScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, serverlessv1alpha1.SchemeBuilder.AddToScheme, verificationv1alpha1.SchemeBuilder.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func request(check *verificationv1alpha1.EndpointCheck) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: check.Namespace, Name: check.Name}}
}

func int32Ptr(value int32) *int32 { return &value }
