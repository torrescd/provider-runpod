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
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
)

// Connect returns an authenticated client after recording ProviderConfig use.
// Namespaced ProviderConfigs may only read a Secret in their own namespace.
func Connect(ctx context.Context, kube client.Client, usage *resource.ProviderConfigUsageTracker, cr resource.ModernManaged) (*clientrunpod.Client, error) {
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

	data, err := resource.CommonCredentialExtractor(ctx, cd.Source, kube, cd.CommonCredentialSelectors)
	if err != nil {
		return nil, err
	}
	return clientrunpod.New(data)
}
