// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package runpod implements the minimal official RunPod REST v1 contract used
// by this provider. It is intentionally hand-written from the public API docs.
package runpod

// TemplateInput is the private Serverless subset accepted by this provider.
type TemplateInput struct {
	Name              string   `json:"name"`
	ImageName         string   `json:"imageName"`
	IsPublic          bool     `json:"isPublic"`
	IsServerless      bool     `json:"isServerless,omitempty"`
	ContainerDiskInGB *int32   `json:"containerDiskInGb,omitempty"`
	DockerEntrypoint  []string `json:"dockerEntrypoint,omitempty"`
	DockerStartCmd    []string `json:"dockerStartCmd,omitempty"`
	Ports             []string `json:"ports,omitempty"`
	VolumeInGB        *int32   `json:"volumeInGb,omitempty"`
	VolumeMountPath   string   `json:"volumeMountPath,omitempty"`
}

type Template struct {
	ID                string   `json:"id"`
	Name              string   `json:"name"`
	ImageName         string   `json:"imageName"`
	IsPublic          bool     `json:"isPublic"`
	IsServerless      bool     `json:"isServerless"`
	ContainerDiskInGB *int32   `json:"containerDiskInGb,omitempty"`
	DockerEntrypoint  []string `json:"dockerEntrypoint,omitempty"`
	DockerStartCmd    []string `json:"dockerStartCmd,omitempty"`
	Ports             []string `json:"ports,omitempty"`
	VolumeInGB        *int32   `json:"volumeInGb,omitempty"`
	VolumeMountPath   string   `json:"volumeMountPath,omitempty"`
}

// EndpointInput always describes a GPU endpoint with zero minimum workers.
type EndpointInput struct {
	Name                string   `json:"name,omitempty"`
	TemplateID          string   `json:"templateId"`
	ComputeType         string   `json:"computeType"`
	GPUCount            int32    `json:"gpuCount"`
	GPUTypeIDs          []string `json:"gpuTypeIds"`
	AllowedCUDAVersions []string `json:"allowedCudaVersions,omitempty"`
	DataCenterIDs       []string `json:"dataCenterIds,omitempty"`
	WorkersMin          int32    `json:"workersMin"`
	WorkersMax          int32    `json:"workersMax"`
	IdleTimeout         int32    `json:"idleTimeout"`
	ScalerType          string   `json:"scalerType"`
	ScalerValue         int32    `json:"scalerValue"`
	ExecutionTimeoutMS  int32    `json:"executionTimeoutMs"`
}

type Endpoint struct {
	ID                  string   `json:"id"`
	Name                string   `json:"name"`
	TemplateID          string   `json:"templateId"`
	ComputeType         string   `json:"computeType"`
	GPUCount            int32    `json:"gpuCount"`
	GPUTypeIDs          []string `json:"gpuTypeIds"`
	AllowedCUDAVersions []string `json:"allowedCudaVersions,omitempty"`
	DataCenterIDs       []string `json:"dataCenterIds,omitempty"`
	WorkersMin          int32    `json:"workersMin"`
	WorkersMax          int32    `json:"workersMax"`
	IdleTimeout         int32    `json:"idleTimeout"`
	ScalerType          string   `json:"scalerType"`
	ScalerValue         int32    `json:"scalerValue"`
	ExecutionTimeoutMS  int32    `json:"executionTimeoutMs"`
	Version             int32    `json:"version,omitempty"`
}
