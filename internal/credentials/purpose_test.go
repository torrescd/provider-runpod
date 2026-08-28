// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package credentials

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRequirePurpose(t *testing.T) {
	management := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{PurposeLabel: PurposeManagement}}}
	inference := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{PurposeLabel: PurposeInference}}}

	if err := RequirePurpose(management, PurposeManagement); err != nil {
		t.Fatalf("management Secret rejected: %v", err)
	}
	if err := RequirePurpose(inference, PurposeInference); err != nil {
		t.Fatalf("inference Secret rejected: %v", err)
	}
	for name, secret := range map[string]*corev1.Secret{"unlabelled": {}, "wrong-purpose": management, "nil": nil} {
		if err := RequirePurpose(secret, PurposeInference); err == nil {
			t.Fatalf("%s Secret accepted for inference", name)
		}
	}
}
