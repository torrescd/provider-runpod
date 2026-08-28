// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package management contains the management-plane credential boundary shared
// by declarative RunPod resources. Verification and model routing do not import
// this package.
package management

import (
	"context"
	"errors"

	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
	"github.com/torrescd/provider-runpod/internal/credentials"
)

// Connect returns an authenticated client after recording ProviderConfig use.
// Namespaced ProviderConfigs may only read a Secret in their own namespace.
// kube should be the manager API reader so credential reads bypass caches.
func Connect(ctx context.Context, kube client.Reader, usage *resource.ProviderConfigUsageTracker, cr resource.ModernManaged) (*clientrunpod.Client, error) {
	if err := usage.Track(ctx, cr); err != nil {
		return nil, err
	}
	ref := cr.GetProviderConfigReference()
	if ref == nil {
		return nil, errors.New("providerConfigRef is required")
	}

	var cd apisv1alpha1.ProviderCredentials
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha1.ProviderConfig{}
		if err := kube.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return nil, err
		}
		cd = pc.Spec.Credentials
		if cd.SecretRef == nil || cd.SecretRef.Namespace != cr.GetNamespace() {
			return nil, errors.New("namespaced ProviderConfig secret must be in the managed resource namespace")
		}
	case "ClusterProviderConfig":
		pc := &apisv1alpha1.ClusterProviderConfig{}
		if err := kube.Get(ctx, types.NamespacedName{Name: ref.Name}, pc); err != nil {
			return nil, err
		}
		cd = pc.Spec.Credentials
	default:
		return nil, errors.New("unsupported providerConfigRef kind")
	}

	data, err := loadCredential(ctx, kube, cd)
	if err != nil {
		return nil, err
	}
	return clientrunpod.New(data)
}

func loadCredential(ctx context.Context, kube client.Reader, cd apisv1alpha1.ProviderCredentials) ([]byte, error) {
	if cd.Source != xpv2.CredentialsSourceSecret || cd.SecretRef == nil {
		return nil, errors.New("ProviderConfig credentials must use a Secret")
	}
	secret := &corev1.Secret{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: cd.SecretRef.Namespace, Name: cd.SecretRef.Name}, secret); err != nil {
		return nil, errors.New("cannot read management credential Secret")
	}
	if err := credentials.RequirePurpose(secret, credentials.PurposeManagement); err != nil {
		return nil, err
	}
	data, ok := secret.Data[cd.SecretRef.Key]
	if !ok {
		return nil, errors.New("management credential Secret key is absent")
	}
	return append([]byte(nil), data...), nil
}
