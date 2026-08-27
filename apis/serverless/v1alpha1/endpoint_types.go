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
	_ resource.ModernManaged = &Endpoint{}
	_ resource.ManagedList   = &EndpointList{}
)

// EndpointParameters are the bounded RunPod Serverless endpoint fields.
// API source: https://docs.runpod.io/api-reference/endpoints/POST/endpoints
type EndpointParameters struct {
	// MaxLifetimeSeconds is the hard lifetime of this experiment endpoint.
	// The provider janitor removes EndpointChecks first, then deletes this CR.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="maxLifetimeSeconds is immutable"
	MaxLifetimeSeconds int32 `json:"maxLifetimeSeconds"`

	// DeleteReferencedTemplateOnExpiry deletes the referenced Kubernetes
	// Template CR after route withdrawal when the endpoint expires. It has no
	// effect when templateId is used directly.
	// +kubebuilder:default=true
	// +optional
	DeleteReferencedTemplateOnExpiry *bool `json:"deleteReferencedTemplateOnExpiry,omitempty"`

	// Name is the endpoint's human-readable name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=191
	Name string `json:"name"`

	// TemplateID is the external RunPod template ID. Prefer TemplateIDRef.
	// +optional
	TemplateID string `json:"templateId,omitempty"`

	// TemplateIDRef references a Template in this namespace.
	// +optional
	TemplateIDRef *xpv2.Reference `json:"templateIdRef,omitempty"`

	// GPUTypeIDs is an explicit, operator-approved GPU type allowlist.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	GPUTypeIDs []string `json:"gpuTypeIds"`

	// AllowedCUDAVersions limits workers to compatible CUDA versions.
	// +kubebuilder:validation:MaxItems=8
	// +optional
	AllowedCUDAVersions []string `json:"allowedCudaVersions,omitempty"`

	// DataCenterIDs limits placement to approved data centers.
	// +kubebuilder:validation:MaxItems=16
	// +optional
	DataCenterIDs []string `json:"dataCenterIds,omitempty"`

	// WorkersMax is deliberately capped for experiment cost control.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1
	WorkersMax int32 `json:"workersMax"`

	// IdleTimeout is the number of seconds before an idle worker is released.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	IdleTimeout int32 `json:"idleTimeout"`

	// ScalerType controls RunPod autoscaling.
	// +kubebuilder:validation:Enum=QUEUE_DELAY;REQUEST_COUNT
	ScalerType string `json:"scalerType"`

	// ScalerValue is the autoscaling target.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	ScalerValue int32 `json:"scalerValue"`

	// ExecutionTimeoutMS bounds an inference request.
	// +kubebuilder:validation:Minimum=1000
	// +kubebuilder:validation:Maximum=600000
	ExecutionTimeoutMS int32 `json:"executionTimeoutMs"`
}

type EndpointObservation struct {
	ID             string      `json:"id,omitempty"`
	TemplateID     string      `json:"templateId,omitempty"`
	WorkersMin     int32       `json:"workersMin,omitempty"`
	WorkersMax     int32       `json:"workersMax,omitempty"`
	InferenceURL   string      `json:"inferenceUrl,omitempty"`
	LastObservedAt metav1.Time `json:"lastObservedAt,omitempty"`
}

type EndpointSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              EndpointParameters `json:"forProvider"`
}

type EndpointStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`
	AtProvider                 EndpointObservation `json:"atProvider,omitempty"`
}

// Endpoint is a bounded RunPod Serverless endpoint. WorkersMin is always zero.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,runpod}
type Endpoint struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EndpointSpec   `json:"spec"`
	Status EndpointStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type EndpointList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Endpoint `json:"items"`
}

var (
	EndpointKind             = reflect.TypeOf(Endpoint{}).Name()
	EndpointGroupKind        = schema.GroupKind{Group: Group, Kind: EndpointKind}.String()
	EndpointKindAPIVersion   = EndpointKind + "." + SchemeGroupVersion.String()
	EndpointGroupVersionKind = SchemeGroupVersion.WithKind(EndpointKind)
)

func init() { SchemeBuilder.Register(&Endpoint{}, &EndpointList{}) }
