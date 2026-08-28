// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package management

import (
	"context"
	"testing"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/credentials"
)

func TestLoadCredentialRequiresManagementPurpose(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	selector := &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "management", Namespace: "runpod-system"},
		Key:             "token",
	}
	cd := apisv1alpha1.ProviderCredentials{
		Source:    xpv2.CredentialsSourceSecret,
		SecretRef: selector,
	}

	for name, purpose := range map[string]string{
		"inference":  credentials.PurposeInference,
		"unlabelled": "",
	} {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "management", Namespace: "runpod-system"},
			Data:       map[string][]byte{"token": []byte("management-token")},
		}
		if purpose != "" {
			secret.Labels = map[string]string{credentials.PurposeLabel: purpose}
		}
		kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
		if _, err := loadCredential(context.Background(), kube, cd); err == nil {
			t.Fatalf("%s Secret accepted for management", name)
		}
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "management", Namespace: "runpod-system",
			Labels: map[string]string{credentials.PurposeLabel: credentials.PurposeManagement},
		},
		Data: map[string][]byte{"token": []byte("management-token")},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	got, err := loadCredential(context.Background(), kube, cd)
	if err != nil || string(got) != "management-token" {
		t.Fatalf("management Secret rejected: data=%q err=%v", got, err)
	}
}
