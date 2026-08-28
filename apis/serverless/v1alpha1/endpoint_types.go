// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package v1alpha1

import (
	"reflect"
	"time"

	resource "github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// FlashBootEvidenceMaxAge bounds the management-API-only interval in which a
// successful flashboot=false write can be trusted. RunPod v1 GET omits the
// field and its Endpoint version does not change for a FlashBoot-only edit.
const FlashBootEvidenceMaxAge = 5 * time.Minute

var (
	_ resource.ModernManaged = &Endpoint{}
	_ resource.ManagedList   = &EndpointList{}
)

// EndpointParameters are the bounded RunPod Serverless endpoint fields.
// API source: https://docs.runpod.io/api-reference/endpoints/POST/endpoints
// +kubebuilder:validation:XValidation:rule="has(self.templateIdRef)",message="templateIdRef is required"
// +kubebuilder:validation:XValidation:rule="self.dataCenterIds.all(id, id.matches('^[A-Za-z0-9_-]+$'))",message="dataCenterIds must be explicit RunPod identifiers"
type EndpointParameters struct {
	// MaxLifetimeSeconds is the hard lifetime of this experiment endpoint.
	// The provider janitor removes EndpointChecks first, then deletes this CR.
	// +kubebuilder:validation:Minimum=60
	// +kubebuilder:validation:Maximum=86400
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="maxLifetimeSeconds is immutable"
	MaxLifetimeSeconds int32 `json:"maxLifetimeSeconds"`

	// DeleteReferencedTemplateOnExpiry deletes the referenced Kubernetes
	// Template CR after route withdrawal when the endpoint expires. It has no
	// effect when no reference is configured (which admission rejects).
	// +kubebuilder:default=true
	// +optional
	DeleteReferencedTemplateOnExpiry *bool `json:"deleteReferencedTemplateOnExpiry,omitempty"`

	// Name is the endpoint's human-readable name.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=191
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9][A-Za-z0-9._-]*$`
	Name string `json:"name"`

	// TemplateIDRef references a Template in this namespace.
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
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=16
	DataCenterIDs []string `json:"dataCenterIds"`

	// WorkersMin is exactly one. The secured route cannot safely admit the
	// first ordinary request to a newly placed scale-to-zero worker before the
	// management API has exposed its cost, storage, and Secure Cloud fields.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	WorkersMin int32 `json:"workersMin"`

	// WorkersMax is exactly one for experiment cost control.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	WorkersMax int32 `json:"workersMax"`

	// MaxWorkerCostMilliUSDPerHour is a local fail-closed ceiling applied to
	// every active worker's official cost fields. It is not sent to RunPod.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000
	MaxWorkerCostMilliUSDPerHour int32 `json:"maxWorkerCostMilliUsdPerHour"`

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
	ID         string `json:"id,omitempty"`
	TemplateID string `json:"templateId,omitempty"`
	// TemplateResourceUID, TemplateResourceGeneration, and TemplateImageDigest
	// bind endpoint readiness to the exact managed Template revision whose
	// rollout was observed. Consumers direct-read that Template before routing.
	TemplateResourceUID        string `json:"templateResourceUid,omitempty"`
	TemplateResourceGeneration int64  `json:"templateResourceGeneration,omitempty"`
	TemplateImageDigest        string `json:"templateImageDigest,omitempty"`
	// Version is RunPod's rollout revision. EndpointCheck binds every full
	// model/tool verification to this value before model-router admits a route.
	Version *int32 `json:"version,omitempty"`
	// FlashBootDisabled records a successful provider write of flashboot=false.
	// RunPod's v1 GET response does not publish this field, so the proof is
	// retained only while the external ID and rollout Version remain unchanged.
	FlashBootDisabled        bool        `json:"flashBootDisabled,omitempty"`
	FlashBootEvidenceVersion *int32      `json:"flashBootEvidenceVersion,omitempty"`
	FlashBootLastEnforcedAt  metav1.Time `json:"flashBootLastEnforcedAt,omitempty"`
	// WorkerSecurityValidated records that at least one active worker was
	// directly observed with the bounded image, storage, cost, placement, and
	// Secure Cloud evidence for WorkerSecurityProofVersion. Any subsequent
	// management observation with zero workers clears the proof, so every cold
	// start must complete a new wake -> management-observe handshake before the
	// router can admit traffic.
	WorkerSecurityValidated    bool        `json:"workerSecurityValidated,omitempty"`
	WorkerSecurityProofVersion *int32      `json:"workerSecurityProofVersion,omitempty"`
	WorkerSecurityObservedAt   metav1.Time `json:"workerSecurityObservedAt,omitempty"`
	WorkersMin                 int32       `json:"workersMin,omitempty"`
	WorkersMax                 int32       `json:"workersMax,omitempty"`
	InferenceURL               string      `json:"inferenceUrl,omitempty"`
	LastObservedAt             metav1.Time `json:"lastObservedAt,omitempty"`
}

// FlashBootEvidenceCurrent reports whether the latest successful enforcement
// remains bound to the current rollout and inside the bounded reassertion
// interval. A small future-skew allowance prevents benign clock skew from
// creating a permanent loop while still failing closed on implausible status.
func (o EndpointObservation) FlashBootEvidenceCurrent(now time.Time) bool {
	if !o.FlashBootDisabled || o.FlashBootEvidenceVersion == nil || o.Version == nil ||
		*o.FlashBootEvidenceVersion != *o.Version || o.FlashBootLastEnforcedAt.IsZero() {
		return false
	}
	age := now.Sub(o.FlashBootLastEnforcedAt.Time)
	return age >= -30*time.Second && age <= FlashBootEvidenceMaxAge
}

// WorkerSecurityEvidenceCurrent binds the latest nonempty validated worker
// snapshot to the currently observed Endpoint rollout. setObservation clears
// this state as soon as the management API reports a zero-worker snapshot.
func (o EndpointObservation) WorkerSecurityEvidenceCurrent() bool {
	return o.WorkerSecurityValidated && o.Version != nil && o.WorkerSecurityProofVersion != nil &&
		*o.Version == *o.WorkerSecurityProofVersion && !o.WorkerSecurityObservedAt.IsZero()
}

// EndpointSpec requires the full Crossplane lifecycle. The TTL janitor can
// only guarantee external cleanup when Delete cannot be disabled through an
// observe-only management policy.
// +kubebuilder:validation:XValidation:rule="self.managementPolicies == ['*']",message="bounded Endpoints require managementPolicies exactly ['*']"
type EndpointSpec struct {
	xpv2.ManagedResourceSpec `json:",inline"`
	ForProvider              EndpointParameters `json:"forProvider"`
}

type EndpointStatus struct {
	xpv2.ManagedResourceStatus `json:",inline"`
	AtProvider                 EndpointObservation `json:"atProvider,omitempty"`
}

// Endpoint is a bounded RunPod Serverless endpoint with exactly one worker.
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
