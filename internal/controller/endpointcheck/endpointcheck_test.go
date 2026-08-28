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
	result inference.Result
	err    error
}

func (f fakeVerifier) Verify(context.Context, string) (inference.Result, error) {
	return f.result, f.err
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
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret).Build()
	r := &reconciler{kube: kube, reader: kube}
	_, err := r.Reconcile(context.Background(), request(check))
	if err != nil {
		t.Fatal(err)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), request(check).NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue || !got.Status.AtProvider.ToolCallVerified || got.Status.AtProvider.EndpointID != "endpoint_1" || got.Status.ObservedGeneration != got.Generation || got.Status.AtProvider.CredentialsSecretResourceVersion == "" {
		t.Fatalf("unexpected status: %+v", got.Status)
	}
}

func TestNonInferenceSecretIsRejected(t *testing.T) {
	scheme := endpointCheckScheme(t)
	check := checkObject()
	secret := inferenceSecretObject(credentials.PurposeManagement)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret).Build()
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
	check.Spec.ForProvider.EndpointID = ""
	check.Spec.ForProvider.EndpointIDRef = &xpv2.Reference{Name: "endpoint"}
	endpoint := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: check.Namespace}}
	meta.SetExternalName(endpoint, "endpoint_direct")
	endpoint.Status.SetConditions(xpv2.Available())

	// The cached client deliberately has no Secret or Endpoint. Reconciliation
	// can succeed only if both objects are read through the direct reader.
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(endpoint, inferenceSecretObject(credentials.PurposeInference)).Build()
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

func TestReferencedEndpointMustBeReadyForCurrentGeneration(t *testing.T) {
	scheme := endpointCheckScheme(t)
	check := checkObject()
	check.Spec.ForProvider.EndpointID = ""
	check.Spec.ForProvider.EndpointIDRef = &xpv2.Reference{Name: "endpoint"}
	endpoint := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{
		Name: "endpoint", Namespace: check.Namespace, Generation: 2,
	}}
	meta.SetExternalName(endpoint, "endpoint_stale")
	endpoint.Status.ObservedGeneration = 1
	endpoint.Status.SetConditions(xpv2.Available())
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(endpoint).Build()
	r := &reconciler{reader: reader}
	if _, err := r.resolveEndpoint(context.Background(), check); err == nil {
		t.Fatal("Ready condition from a stale Endpoint generation was accepted")
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
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret).Build()
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
			MaxLifetimeSeconds: 3600, EndpointID: "endpoint_1", ExpectedModelID: "org/model", TimeoutSeconds: 5,
			InferenceCredentialsSecretRef: xpv2.LocalSecretKeySelector{LocalSecretReference: xpv2.LocalSecretReference{Name: "inference"}, Key: "token"},
		}},
	}
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
