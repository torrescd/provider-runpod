// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

const (
	Group   = "verification.runpod.crossplane.io"
	Version = "v1alpha1"
)

var (
	SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}
	SchemeBuilder      = &scheme.Builder{GroupVersion: SchemeGroupVersion}
)
