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
	_ resource.ModernManaged = &Template{}
	_ resource.ManagedList   = &TemplateList{}
)

// TemplateParameters are the supported, security-bounded RunPod template fields.
// API source: https://docs.runpod.io/api-reference/templates/POST/templates
type TemplateParameters struct {
	// MaxLifetimeSeconds bounds this Template even when the dependent Endpoint
	// is never admitted or created. The independent reaper direct-lists all
	// Endpoint references before initiating deletion.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="maxLifetimeSeconds is immutable"
	MaxLifetimeSeconds int32 `json:"maxLifetimeSeconds"`

	// Name is the unique human-readable template name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=191
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name string `json:"name"`

	// ImageName is an OCI image reference pinned to a sha256 digest.
	// Mutable tags are deliberately rejected.
	// +kubebuilder:validation:Pattern=`^[^[:space:]@]+@sha256:[a-f0-9]{64}$`
	ImageName string `json:"imageName"`

	// ContainerDiskInGB is ephemeral container disk capacity.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	// +kubebuilder:default=50
	// +optional
	ContainerDiskInGB *int32 `json:"containerDiskInGb,omitempty"`

	// DockerEntrypoint overrides the image entrypoint.
	// +kubebuilder:validation:MaxItems=32
	// +optional
	DockerEntrypoint []string `json:"dockerEntrypoint,omitempty"`

	// DockerStartCmd overrides the image command.
	// +kubebuilder:validation:MaxItems=64
	// +optional
	DockerStartCmd []string `json:"dockerStartCmd,omitempty"`

	// Ports contains RunPod port declarations such as 8000/http.
	// It is required because RunPod otherwise defaults to 8888/http and 22/tcp.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	// +kubebuilder:validation:items:MaxLength=10
	// +kubebuilder:validation:items:Pattern=`^([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])/(http|tcp)$`
	Ports []string `json:"ports"`

	// VolumeInGB must be explicitly zero. RunPod otherwise defaults an omitted
	// volume to 20 GiB and a /workspace mount; this bounded provider does not
	// support persistent worker volumes.
	// +kubebuilder:validation:Enum=0
	VolumeInGB int32 `json:"volumeInGb"`
}

type TemplateObservation struct {
	ID             string      `json:"id,omitempty"`
	Name           string      `json:"name,omitempty"`
	ImageName      string      `json:"imageName,omitempty"`
	IsServerless   bool        `json:"isServerless,omitempty"`
	LastObservedAt metav1.Time `json:"lastObservedAt,omitempty"`
}

// TemplateSpec requires the full Crossplane lifecycle. A bounded Template may
// not be orphaned when its Kubernetes object is removed by GitOps or the
// Endpoint lifetime reaper.
// +kubebuilder:validation:XValidation:rule="self.managementPolicies == ['*']",message="bounded Templates require managementPolicies exactly ['*']"
type TemplateSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              TemplateParameters `json:"forProvider"`
}

type TemplateStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`
	AtProvider                 TemplateObservation `json:"atProvider,omitempty"`
}

// Template is a private RunPod Serverless template backed by a digest-pinned image.
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:resource:scope=Namespaced,categories={crossplane,managed,runpod}
type Template struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TemplateSpec   `json:"spec"`
	Status TemplateStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type TemplateList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Template `json:"items"`
}

var (
	TemplateKind             = reflect.TypeOf(Template{}).Name()
	TemplateGroupKind        = schema.GroupKind{Group: Group, Kind: TemplateKind}.String()
	TemplateKindAPIVersion   = TemplateKind + "." + SchemeGroupVersion.String()
	TemplateGroupVersionKind = SchemeGroupVersion.WithKind(TemplateKind)
)

func init() { SchemeBuilder.Register(&Template{}, &TemplateList{}) }
