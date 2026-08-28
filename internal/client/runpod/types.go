// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package runpod implements the minimal official RunPod REST v1 contract used
// by this provider. It is intentionally hand-written from the public API docs.
package runpod

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
)

// TemplateInput is the private Serverless subset accepted by this provider.
type TemplateInput struct {
	Name                    string            `json:"name"`
	Category                string            `json:"category"`
	ImageName               string            `json:"imageName"`
	IsPublic                bool              `json:"isPublic"`
	IsServerless            bool              `json:"isServerless,omitempty"`
	ContainerDiskInGB       int32             `json:"containerDiskInGb"`
	DockerEntrypoint        []string          `json:"dockerEntrypoint"`
	DockerStartCmd          []string          `json:"dockerStartCmd"`
	Ports                   []string          `json:"ports"`
	Readme                  string            `json:"readme"`
	VolumeInGB              int32             `json:"volumeInGb"`
	VolumeMountPath         string            `json:"volumeMountPath"`
	Env                     map[string]string `json:"env"`
	ContainerRegistryAuthID string            `json:"containerRegistryAuthId"`
}

// TemplateUpdateInput excludes the create-only category and isServerless
// fields from the official v1 PATCH contract.
type TemplateUpdateInput struct {
	Name                    string            `json:"name"`
	ImageName               string            `json:"imageName"`
	IsPublic                bool              `json:"isPublic"`
	ContainerDiskInGB       int32             `json:"containerDiskInGb"`
	DockerEntrypoint        []string          `json:"dockerEntrypoint"`
	DockerStartCmd          []string          `json:"dockerStartCmd"`
	Ports                   []string          `json:"ports"`
	Readme                  string            `json:"readme"`
	VolumeInGB              int32             `json:"volumeInGb"`
	VolumeMountPath         string            `json:"volumeMountPath"`
	Env                     map[string]string `json:"env"`
	ContainerRegistryAuthID string            `json:"containerRegistryAuthId"`
}

type Template struct {
	ID                      string            `json:"id"`
	Name                    string            `json:"name"`
	Category                string            `json:"category"`
	ImageName               string            `json:"imageName"`
	IsPublic                *bool             `json:"isPublic"`
	IsRunpod                *bool             `json:"isRunpod"`
	IsServerless            *bool             `json:"isServerless"`
	ContainerDiskInGB       *int32            `json:"containerDiskInGb,omitempty"`
	DockerEntrypoint        []string          `json:"dockerEntrypoint,omitempty"`
	DockerStartCmd          []string          `json:"dockerStartCmd,omitempty"`
	Ports                   []string          `json:"ports,omitempty"`
	Readme                  NullableString    `json:"readme"`
	VolumeInGB              *int32            `json:"volumeInGb,omitempty"`
	VolumeMountPath         NullableString    `json:"volumeMountPath"`
	Env                     map[string]string `json:"env,omitempty"`
	ContainerRegistryAuthID NullableString    `json:"containerRegistryAuthId"`
}

// EndpointInput always describes a GPU endpoint with zero minimum workers.
type EndpointInput struct {
	Name                string   `json:"name,omitempty"`
	TemplateID          string   `json:"templateId"`
	ComputeType         string   `json:"computeType"`
	GPUCount            int32    `json:"gpuCount"`
	GPUTypeIDs          []string `json:"gpuTypeIds"`
	AllowedCUDAVersions []string `json:"allowedCudaVersions,omitempty"`
	DataCenterIDs       []string `json:"dataCenterIds"`
	WorkersMin          int32    `json:"workersMin"`
	WorkersMax          int32    `json:"workersMax"`
	IdleTimeout         int32    `json:"idleTimeout"`
	ScalerType          string   `json:"scalerType"`
	ScalerValue         int32    `json:"scalerValue"`
	ExecutionTimeoutMS  int32    `json:"executionTimeoutMs"`
	// FlashBoot retains worker state and defaults on in RunPod. This provider's
	// ephemeral contract always serializes false rather than trusting that
	// external default.
	FlashBoot        bool     `json:"flashboot"`
	NetworkVolumeID  string   `json:"networkVolumeId,omitempty"`
	NetworkVolumeIDs []string `json:"networkVolumeIds,omitempty"`
	MinCUDAVersion   string   `json:"minCudaVersion,omitempty"`
}

// EndpointUpdateInput is the official v1 PATCH subset. In particular,
// computeType is create-only and must not be sent during the five-minute
// bounded-state/FlashBoot reassertion.
type EndpointUpdateInput struct {
	Name                string   `json:"name,omitempty"`
	TemplateID          string   `json:"templateId"`
	GPUCount            int32    `json:"gpuCount"`
	GPUTypeIDs          []string `json:"gpuTypeIds"`
	AllowedCUDAVersions []string `json:"allowedCudaVersions,omitempty"`
	DataCenterIDs       []string `json:"dataCenterIds"`
	WorkersMin          int32    `json:"workersMin"`
	WorkersMax          int32    `json:"workersMax"`
	IdleTimeout         int32    `json:"idleTimeout"`
	ScalerType          string   `json:"scalerType"`
	ScalerValue         int32    `json:"scalerValue"`
	ExecutionTimeoutMS  int32    `json:"executionTimeoutMs"`
	FlashBoot           bool     `json:"flashboot"`
	NetworkVolumeID     string   `json:"networkVolumeId,omitempty"`
	NetworkVolumeIDs    []string `json:"networkVolumeIds,omitempty"`
	MinCUDAVersion      string   `json:"minCudaVersion,omitempty"`
}

type Endpoint struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	TemplateID          string            `json:"templateId"`
	ComputeType         string            `json:"computeType"`
	GPUCount            int32             `json:"gpuCount"`
	GPUTypeIDs          []string          `json:"gpuTypeIds"`
	AllowedCUDAVersions []string          `json:"allowedCudaVersions,omitempty"`
	DataCenterIDs       []string          `json:"dataCenterIds,omitempty"`
	WorkersMin          *int32            `json:"workersMin"`
	WorkersMax          *int32            `json:"workersMax"`
	IdleTimeout         int32             `json:"idleTimeout"`
	ScalerType          string            `json:"scalerType"`
	ScalerValue         int32             `json:"scalerValue"`
	ExecutionTimeoutMS  int32             `json:"executionTimeoutMs"`
	FlashBoot           *bool             `json:"flashboot,omitempty"`
	NetworkVolumeID     NullableString    `json:"networkVolumeId"`
	NetworkVolumeIDs    []string          `json:"networkVolumeIds,omitempty"`
	MinCUDAVersion      NullableString    `json:"minCudaVersion"`
	Env                 map[string]string `json:"env,omitempty"`
	InstanceIDs         []string          `json:"instanceIds,omitempty"`
	Version             *int32            `json:"version"`
	Workers             []EndpointWorker  `json:"workers"`
	Template            *Template         `json:"template"`
}

// EndpointWorker is the security-relevant subset of the official Pod schema
// returned for GET /endpoints/{id}?includeWorkers=true. Pointer and nil-able
// fields intentionally retain JSON presence: an omitted cost, identity, image,
// or storage field cannot be mistaken for a safe zero value.
type EndpointWorker struct {
	ID                      NullableString         `json:"id"`
	EndpointID              NullableString         `json:"endpointId"`
	TemplateID              NullableString         `json:"templateId"`
	AdjustedCostPerHr       NullableDecimal        `json:"adjustedCostPerHr"`
	CostPerHr               NullableDecimal        `json:"costPerHr"`
	DesiredStatus           NullableString         `json:"desiredStatus"`
	Interruptible           *bool                  `json:"interruptible"`
	PublicIP                NullableString         `json:"publicIp"`
	PortMappings            json.RawMessage        `json:"portMappings"`
	SavingsPlans            json.RawMessage        `json:"savingsPlans"`
	SLSVersion              *int32                 `json:"slsVersion"`
	Image                   NullableString         `json:"image"`
	ContainerDiskInGB       *int32                 `json:"containerDiskInGb"`
	ContainerRegistryAuthID NullableString         `json:"containerRegistryAuthId"`
	DockerEntrypoint        []string               `json:"dockerEntrypoint"`
	DockerStartCmd          []string               `json:"dockerStartCmd"`
	Ports                   []string               `json:"ports"`
	Env                     map[string]string      `json:"env"`
	VolumeInGB              *int32                 `json:"volumeInGb"`
	VolumeMountPath         NullableString         `json:"volumeMountPath"`
	VolumeEncrypted         *bool                  `json:"volumeEncrypted"`
	NetworkVolume           json.RawMessage        `json:"networkVolume"`
	GPU                     *EndpointWorkerGPU     `json:"gpu"`
	Machine                 *EndpointWorkerMachine `json:"machine"`
	Locked                  *bool                  `json:"locked"`
}

// NullableString distinguishes omission from the explicit null used by the
// official RunPod worker example. Omission is not evidence of a safe value;
// explicit null is accepted only for fields whose documented meaning is none.
type NullableString struct {
	Present bool
	Null    bool
	Value   string
}

func StringValue(value string) NullableString {
	return NullableString{Present: true, Value: value}
}

func NullString() NullableString {
	return NullableString{Present: true, Null: true}
}

func (s *NullableString) UnmarshalJSON(data []byte) error {
	s.Present = true
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		s.Null = true
		s.Value = ""
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return errors.New("expected a string or null")
	}
	s.Null = false
	s.Value = value
	return nil
}

// NullableDecimal retains omission/null while accepting both decimal JSON
// numbers and the decimal strings used by RunPod's published worker example.
type NullableDecimal struct {
	Present bool
	Null    bool
	Value   float64
}

func DecimalValue(value float64) NullableDecimal {
	return NullableDecimal{Present: true, Value: value}
}

func (d *NullableDecimal) UnmarshalJSON(data []byte) error {
	d.Present = true
	trimmed := bytes.TrimSpace(data)
	if bytes.Equal(trimmed, []byte("null")) {
		d.Null = true
		d.Value = 0
		return nil
	}
	valueText := string(trimmed)
	if len(trimmed) > 0 && trimmed[0] == '"' {
		var encoded string
		if err := json.Unmarshal(trimmed, &encoded); err != nil {
			return errors.New("expected a decimal string, number, or null")
		}
		valueText = encoded
	}
	value, err := strconv.ParseFloat(valueText, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return errors.New("expected a finite decimal string, number, or null")
	}
	d.Null = false
	d.Value = value
	return nil
}

type EndpointWorkerGPU struct {
	Count *int32 `json:"count"`
}

type EndpointWorkerMachine struct {
	GPUTypeID          *string         `json:"gpuTypeId"`
	DataCenterID       *string         `json:"dataCenterId"`
	SecureCloud        *bool           `json:"secureCloud"`
	CostPerHr          NullableDecimal `json:"costPerHr"`
	CurrentPricePerGPU NullableDecimal `json:"currentPricePerGpu"`
}

// UnmarshalJSON accepts both dataCenterIds encodings published by RunPod's
// official v1 endpoint contract: an OpenAPI string array and a comma-separated
// string used by the live GET/POST examples. The controller always observes a
// trimmed string slice.
func (e *Endpoint) UnmarshalJSON(data []byte) error {
	fields := map[string]json.RawMessage{}
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	rawDataCenterIDs := fields["dataCenterIds"]
	delete(fields, "dataCenterIds")
	withoutDataCenterIDs, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	type plainEndpoint Endpoint
	var decoded plainEndpoint
	if err := json.Unmarshal(withoutDataCenterIDs, &decoded); err != nil {
		return err
	}
	ids, err := decodeDataCenterIDs(rawDataCenterIDs)
	if err != nil {
		return err
	}
	*e = Endpoint(decoded)
	e.DataCenterIDs = ids
	return nil
}

func decodeDataCenterIDs(raw json.RawMessage) ([]string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	var values []string
	if trimmed[0] == '[' {
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, errors.New("dataCenterIds must be a string array or comma-separated string")
		}
	} else {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, errors.New("dataCenterIds must be a string array or comma-separated string")
		}
		if strings.TrimSpace(value) == "" {
			return nil, nil
		}
		values = strings.Split(value, ",")
	}
	for i := range values {
		values[i] = strings.TrimSpace(values[i])
		if values[i] == "" || strings.ContainsAny(values[i], "\r\n\t,") {
			return nil, errors.New("dataCenterIds contains an empty or malformed identifier")
		}
	}
	return values, nil
}
