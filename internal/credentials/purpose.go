// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package credentials defines the mandatory boundary between RunPod
// management credentials and endpoint-scoped inference credentials.
package credentials

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

const (
	// PurposeLabel is required on every Secret consumed by provider-runpod.
	PurposeLabel = "runpod.crossplane.io/credential-purpose"

	// PurposeManagement labels a Secret that may be consumed by the provider
	// management process, but never by model-router.
	PurposeManagement = "management"

	// PurposeInference labels a Secret that may be consumed by model-router and
	// its EndpointCheck controller, but never by the provider process.
	PurposeInference = "inference"
)

// RequirePurpose fails closed when a Secret is missing the exact purpose label.
func RequirePurpose(secret *corev1.Secret, expected string) error {
	if secret == nil || secret.Labels[PurposeLabel] != expected {
		return fmt.Errorf("Secret must have label %q=%q", PurposeLabel, expected)
	}
	return nil
}
