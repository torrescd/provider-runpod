// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpointcheck

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
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
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "inference", Namespace: "runpod-system"}, Data: map[string][]byte{"token": []byte("endpoint-only")}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret).Build()
	r := &reconciler{kube: kube}
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

func TestManagementSecretIsRejected(t *testing.T) {
	scheme := endpointCheckScheme(t)
	check := checkObject()
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "inference", Namespace: "runpod-system"}, Data: map[string][]byte{"token": []byte("same-key")}}
	pc := &apisv1alpha1.ProviderConfig{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "runpod-system"}, Spec: apisv1alpha1.ProviderConfigSpec{Credentials: apisv1alpha1.ProviderCredentials{Source: xpv2.CredentialsSourceSecret, CommonCredentialSelectors: xpv2.CommonCredentialSelectors{SecretRef: &xpv2.SecretKeySelector{SecretReference: xpv2.SecretReference{Name: "inference", Namespace: "runpod-system"}, Key: "token"}}}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(check).WithObjects(check, secret, pc).Build()
	r := &reconciler{kube: kube}
	_, err := r.Reconcile(context.Background(), request(check))
	if err != nil {
		t.Fatal(err)
	}
	got := &verificationv1alpha1.EndpointCheck{}
	_ = kube.Get(context.Background(), request(check).NamespacedName, got)
	if got.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionFalse {
		t.Fatal("management secret was admitted")
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
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, apisv1alpha1.SchemeBuilder.AddToScheme, verificationv1alpha1.SchemeBuilder.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func request(check *verificationv1alpha1.EndpointCheck) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Namespace: check.Namespace, Name: check.Name}}
}
