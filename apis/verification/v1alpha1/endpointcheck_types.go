// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package v1alpha1

import (
	"reflect"

	resource "github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	_ resource.ModernManaged = &EndpointCheck{}
	_ resource.ManagedList   = &EndpointCheckList{}
)

// EndpointCheckParameters define an authenticated, inference-only readiness gate.
// +kubebuilder:validation:XValidation:rule="has(self.endpointId) != has(self.endpointIdRef)",message="exactly one of endpointId or endpointIdRef is required"
type EndpointCheckParameters struct {
	// MaxLifetimeSeconds is the hard lifetime of the admitted route. The check
	// controller self-deletes the CR at expiry, causing model-router to fail closed.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="maxLifetimeSeconds is immutable"
	MaxLifetimeSeconds int32 `json:"maxLifetimeSeconds"`

	// EndpointID is the external RunPod endpoint ID. Prefer EndpointIDRef.
	// +kubebuilder:validation:MinLength=1
	// +optional
	EndpointID string `json:"endpointId,omitempty"`

	// EndpointIDRef references an Endpoint in this namespace.
	// +optional
	EndpointIDRef *xpv2.Reference `json:"endpointIdRef,omitempty"`

	// ExpectedModelID is required to appear exactly in the OpenAI models response.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	ExpectedModelID string `json:"expectedModelId"`

	// InferenceCredentialsSecretRef contains an endpoint-scoped token. It must
	// never reference the management-key Secret used by ProviderConfig.
	InferenceCredentialsSecretRef xpv2.LocalSecretKeySelector `json:"inferenceCredentialsSecretRef"`

	// TimeoutSeconds bounds each probe request.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=60
	// +kubebuilder:default=15
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

type EndpointCheckObservation struct {
	EndpointID       string `json:"endpointId,omitempty"`
	Healthy          bool   `json:"healthy,omitempty"`
	ModelVerified    bool   `json:"modelVerified,omitempty"`
	ToolCallVerified bool   `json:"toolCallVerified,omitempty"`
	InferenceURL     string `json:"inferenceUrl,omitempty"`
	// CredentialsSecretResourceVersion binds admission to the exact Secret
	// version used by the authenticated checks. It is metadata, never secret
	// material. A credential rotation withdraws routing until it is reverified.
	CredentialsSecretResourceVersion string      `json:"credentialsSecretResourceVersion,omitempty"`
	LastCheckedAt                    metav1.Time `json:"lastCheckedAt,omitempty"`
}

type EndpointCheckSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              EndpointCheckParameters `json:"forProvider"`
}

type EndpointCheckStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`
	AtProvider                 EndpointCheckObservation `json:"atProvider,omitempty"`
}

// EndpointCheck gates model routing on authenticated health, model identity,
// and deterministic tool-call checks. It never consumes a management API key.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="ENDPOINT",type="string",JSONPath=".spec.forProvider.endpointId"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,runpod}
type EndpointCheck struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EndpointCheckSpec   `json:"spec"`
	Status EndpointCheckStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type EndpointCheckList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EndpointCheck `json:"items"`
}

var (
	EndpointCheckKind             = reflect.TypeOf(EndpointCheck{}).Name()
	EndpointCheckGroupKind        = schema.GroupKind{Group: Group, Kind: EndpointCheckKind}.String()
	EndpointCheckKindAPIVersion   = EndpointCheckKind + "." + SchemeGroupVersion.String()
	EndpointCheckGroupVersionKind = SchemeGroupVersion.WithKind(EndpointCheckKind)
)

func init() { SchemeBuilder.Register(&EndpointCheck{}, &EndpointCheckList{}) }
