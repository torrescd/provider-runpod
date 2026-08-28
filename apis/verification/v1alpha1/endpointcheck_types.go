// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package v1alpha1

import (
	"reflect"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// EndpointCheckParameters define an authenticated, inference-only readiness gate.
// +kubebuilder:validation:XValidation:rule="has(self.endpointIdRef)",message="endpointIdRef is required"
type EndpointCheckParameters struct {
	// MaxLifetimeSeconds is the hard lifetime of the admitted route. The check
	// controller self-deletes the CR at expiry, causing model-router to fail closed.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="maxLifetimeSeconds is immutable"
	MaxLifetimeSeconds int32 `json:"maxLifetimeSeconds"`

	// EndpointIDRef references an Endpoint in this namespace.
	EndpointIDRef *xpv2.Reference `json:"endpointIdRef,omitempty"`

	// ExpectedModelID is required to appear exactly in the OpenAI models response.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=512
	ExpectedModelID string `json:"expectedModelId"`

	// InferenceCredentialsSecretRef contains an endpoint-scoped token. The
	// referenced Secret must be labelled
	// runpod.crossplane.io/credential-purpose=inference.
	InferenceCredentialsSecretRef xpv2.LocalSecretKeySelector `json:"inferenceCredentialsSecretRef"`

	// VerificationIntervalSeconds is the minimum interval between authenticated
	// model/tool-call verifications in steady state. Kubernetes Endpoint status
	// and credential metadata are checked every 30 seconds without invoking GPU
	// inference. Relevant generation, endpoint, or credential changes bypass the
	// interval and require immediate full re-verification.
	// +kubebuilder:validation:Minimum=900
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:default=3600
	VerificationIntervalSeconds int32 `json:"verificationIntervalSeconds,omitempty"`

	// TimeoutSeconds bounds each individual full-verification request. The
	// default tolerates initial worker provisioning and model loading.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=600
	// +kubebuilder:default=180
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

type EndpointCheckObservation struct {
	EndpointID string `json:"endpointId,omitempty"`
	// EndpointResourceUID and EndpointResourceGeneration bind a verification to
	// the exact referenced Kubernetes Endpoint identity and desired revision.
	// EndpointVersion binds it to RunPod's observed rollout revision.
	EndpointResourceUID        string `json:"endpointResourceUid,omitempty"`
	EndpointResourceGeneration int64  `json:"endpointResourceGeneration,omitempty"`
	EndpointVersion            *int32 `json:"endpointVersion,omitempty"`
	TemplateResourceUID        string `json:"templateResourceUid,omitempty"`
	TemplateResourceGeneration int64  `json:"templateResourceGeneration,omitempty"`
	TemplateImageDigest        string `json:"templateImageDigest,omitempty"`
	Healthy                    bool   `json:"healthy,omitempty"`
	ModelVerified              bool   `json:"modelVerified,omitempty"`
	ToolCallVerified           bool   `json:"toolCallVerified,omitempty"`
	InferenceURL               string `json:"inferenceUrl,omitempty"`
	// CredentialsSecretResourceVersion binds admission to the exact Secret
	// version used by the authenticated checks. It is metadata, never secret
	// material. A credential rotation withdraws routing until it is reverified.
	CredentialsSecretResourceVersion string `json:"credentialsSecretResourceVersion,omitempty"`
	// LastCheckedAt records the latest cheap control-plane/credential liveness
	// check. LastVerificationAttemptAt records every attempted authenticated
	// health/model/tool verification, including failures and timeouts, so the
	// minimum verification interval also bounds costly retry attempts.
	// LastVerifiedAt records the latest successful full verification.
	LastCheckedAt             metav1.Time `json:"lastCheckedAt,omitempty"`
	LastVerificationAttemptAt metav1.Time `json:"lastVerificationAttemptAt,omitempty"`
	LastVerifiedAt            metav1.Time `json:"lastVerifiedAt,omitempty"`
}

type EndpointCheckSpec struct {
	ForProvider EndpointCheckParameters `json:"forProvider"`
}

type EndpointCheckStatus struct {
	xpv2.ConditionedStatus `json:",inline"`
	// ObservedGeneration is the EndpointCheck generation evaluated by the
	// verifier. It is explicit because this auxiliary gate is not a Crossplane
	// managed resource and has no providerConfig or management policies.
	ObservedGeneration int64                    `json:"observedGeneration,omitempty"`
	AtProvider         EndpointCheckObservation `json:"atProvider,omitempty"`
}

// EndpointCheck gates model routing on authenticated health, model identity,
// and deterministic tool-call checks. It never consumes a management API key.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="ENDPOINT",type="string",JSONPath=".spec.forProvider.endpointIdRef.name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,runpod}
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
