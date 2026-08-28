// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package runpod

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/torrescd/provider-runpod/internal/identifier"
)

const (
	DefaultBaseURL = "https://rest.runpod.io/v1"
	maxBodyBytes   = 1 << 20
	maxAttempts    = 4
)

var (
	ErrNotFound        = errors.New("runpod resource not found")
	ErrAmbiguous       = errors.New("multiple RunPod resources matched recovery lookup")
	ErrCreateAmbiguous = errors.New("RunPod create result is ambiguous")
	safeRequestID      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type APIError struct {
	StatusCode int
	RequestID  string
}

func (e *APIError) Error() string {
	if e.RequestID == "" {
		return fmt.Sprintf("RunPod API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("RunPod API returned HTTP %d (request %s)", e.StatusCode, e.RequestID)
}

type Option func(*Client) error

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	apiKey     string
	sleep      func(context.Context, time.Duration) error
}

func New(rawKey []byte, opts ...Option) (*Client, error) {
	key := string(rawKey)
	if key == "" || key != strings.TrimSpace(key) || len(key) > 4096 || strings.ContainsAny(key, "\r\n") {
		return nil, errors.New("RunPod API key is empty or malformed")
	}
	u, err := url.Parse(DefaultBaseURL)
	if err != nil {
		return nil, errors.New("invalid built-in RunPod API URL")
	}
	c := &Client{
		baseURL: u,
		httpClient: &http.Client{
			Timeout:       30 * time.Second,
			CheckRedirect: rejectRedirect,
			Transport: &http.Transport{
				Proxy:                  http.ProxyFromEnvironment,
				DialContext:            (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				TLSHandshakeTimeout:    5 * time.Second,
				ResponseHeaderTimeout:  20 * time.Second,
				MaxResponseHeaderBytes: 64 << 10,
				IdleConnTimeout:        90 * time.Second,
			},
		},
		apiKey: key,
		sleep: func(ctx context.Context, d time.Duration) error {
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				return nil
			}
		},
	}
	for _, o := range opts {
		if err := o(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func rejectRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// WithHTTPClient replaces the HTTP client, primarily for deterministic tests.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) error {
		if h == nil {
			return errors.New("HTTP client must not be nil")
		}
		c.httpClient = h
		return nil
	}
}

// WithBaseURLForTesting permits only loopback endpoints. Production callers
// cannot redirect the management credential to an arbitrary host.
func WithBaseURLForTesting(raw string) Option {
	return func(c *Client) error {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" {
			return errors.New("invalid test base URL")
		}
		ip := net.ParseIP(u.Hostname())
		if u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("test base URL must be loopback")
		}
		c.baseURL = u
		return nil
	}
}

func (c *Client) GetTemplate(ctx context.Context, id string) (*Template, error) {
	escaped, err := safeID(id)
	if err != nil {
		return nil, err
	}
	var out Template
	if err := c.doJSON(ctx, http.MethodGet, "/templates/"+escaped+"?includeEndpointBoundTemplates=true", nil, &out); err != nil {
		return nil, err
	}
	if err := validateTemplateResponse(&out); err != nil || out.ID != id {
		return nil, errors.New("RunPod Template response has an invalid resource ID")
	}
	return &out, nil
}

func (c *Client) ListTemplates(ctx context.Context) ([]Template, error) {
	var out []Template
	if err := c.doJSON(ctx, http.MethodGet, "/templates?includeEndpointBoundTemplates=true", nil, &out); err != nil {
		return nil, err
	}
	for i := range out {
		if err := validateTemplateResponse(&out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *Client) FindTemplateByName(ctx context.Context, name string) (*Template, error) {
	all, err := c.ListTemplates(ctx)
	if err != nil {
		return nil, err
	}
	var found *Template
	for i := range all {
		if all[i].Name != name {
			continue
		}
		if found != nil {
			return nil, ErrAmbiguous
		}
		found = &all[i]
	}
	if found == nil {
		return nil, ErrNotFound
	}
	return found, nil
}

func (c *Client) CreateTemplate(ctx context.Context, in TemplateInput) (*Template, error) {
	in = normalizeTemplateInput(in)
	var out Template
	if err := c.doJSON(ctx, http.MethodPost, "/templates", in, &out); err != nil {
		return nil, err
	}
	if out.ID == "" {
		return nil, ErrCreateAmbiguous
	}
	if err := validateTemplateResponse(&out); err != nil || out.Name != in.Name {
		return nil, fmt.Errorf("%w: RunPod Template success response has an invalid resource ID", ErrCreateAmbiguous)
	}
	return &out, nil
}

func (c *Client) UpdateTemplate(ctx context.Context, id string, in TemplateInput) (*Template, error) {
	escaped, err := safeID(id)
	if err != nil {
		return nil, err
	}
	// isServerless is create-only in the published update contract.
	in.IsServerless = false
	in = normalizeTemplateInput(in)
	update := templateUpdateInput(in)
	var out Template
	if err := c.doJSON(ctx, http.MethodPatch, "/templates/"+escaped, update, &out); err != nil {
		return nil, err
	}
	if out.ID == "" {
		return c.GetTemplate(ctx, id)
	}
	if err := validateTemplateResponse(&out); err != nil || out.ID != id || out.Name != in.Name {
		return nil, errors.New("RunPod Template update response has an invalid resource ID")
	}
	return &out, nil
}

func (c *Client) DeleteTemplate(ctx context.Context, id string) error {
	escaped, err := safeID(id)
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodDelete, "/templates/"+escaped, nil, nil)
}

func (c *Client) GetEndpoint(ctx context.Context, id string) (*Endpoint, error) {
	escaped, err := safeID(id)
	if err != nil {
		return nil, err
	}
	var out Endpoint
	if err := c.doJSON(ctx, http.MethodGet, "/endpoints/"+escaped+"?includeTemplate=true&includeWorkers=true", nil, &out); err != nil {
		return nil, err
	}
	if err := validateEndpointObservation(&out); err != nil || out.ID != id {
		return nil, errors.New("RunPod Endpoint response has an invalid resource ID")
	}
	return &out, nil
}

func (c *Client) ListEndpoints(ctx context.Context) ([]Endpoint, error) {
	var out []Endpoint
	if err := c.doJSON(ctx, http.MethodGet, "/endpoints?includeTemplate=true&includeWorkers=true", nil, &out); err != nil {
		return nil, err
	}
	for i := range out {
		if err := validateEndpointObservation(&out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *Client) FindEndpointByName(ctx context.Context, name string) (*Endpoint, error) {
	all, err := c.ListEndpoints(ctx)
	if err != nil {
		return nil, err
	}
	var found *Endpoint
	for i := range all {
		if all[i].Name != name {
			continue
		}
		if found != nil {
			return nil, ErrAmbiguous
		}
		found = &all[i]
	}
	if found == nil {
		return nil, ErrNotFound
	}
	return found, nil
}

func (c *Client) CreateEndpoint(ctx context.Context, in EndpointInput) (*Endpoint, error) {
	if err := identifier.ValidateRunPodID(in.TemplateID); err != nil {
		return nil, err
	}
	in = normalizeEndpointInput(in)
	var out Endpoint
	if err := c.doJSON(ctx, http.MethodPost, "/endpoints", in, &out); err != nil {
		return nil, err
	}
	if out.ID == "" {
		return nil, ErrCreateAmbiguous
	}
	if err := validateEndpointObservation(&out); err != nil || out.Name != in.Name || out.TemplateID != in.TemplateID {
		return nil, fmt.Errorf("%w: RunPod Endpoint success response has an invalid resource ID", ErrCreateAmbiguous)
	}
	return &out, nil
}

func (c *Client) UpdateEndpoint(ctx context.Context, id string, in EndpointInput) (*Endpoint, error) {
	escaped, err := safeID(id)
	if err != nil {
		return nil, err
	}
	if err := identifier.ValidateRunPodID(in.TemplateID); err != nil {
		return nil, err
	}
	in = normalizeEndpointInput(in)
	update := endpointUpdateInput(in)
	var out Endpoint
	if err := c.doJSON(ctx, http.MethodPatch, "/endpoints/"+escaped, update, &out); err != nil {
		return nil, err
	}
	if out.ID == "" || out.TemplateID == "" || out.WorkersMin == nil || out.WorkersMax == nil || out.Version == nil || out.Workers == nil || out.Template == nil {
		return c.GetEndpoint(ctx, id)
	}
	if err := validateEndpointObservation(&out); err != nil || out.ID != id || out.Name != in.Name || out.TemplateID != in.TemplateID {
		return nil, errors.New("RunPod Endpoint update response has an invalid resource ID")
	}
	return &out, nil
}

func (c *Client) DeleteEndpoint(ctx context.Context, id string) error {
	escaped, err := safeID(id)
	if err != nil {
		return err
	}
	// RunPod requires both worker bounds to be zero before endpoint deletion.
	zero := struct {
		WorkersMin int32 `json:"workersMin"`
		WorkersMax int32 `json:"workersMax"`
	}{}
	if err := c.doJSON(ctx, http.MethodPatch, "/endpoints/"+escaped, zero, nil); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return c.doJSON(ctx, http.MethodDelete, "/endpoints/"+escaped, nil, nil)
}

func safeID(id string) (string, error) {
	if err := identifier.ValidateRunPodID(id); err != nil {
		return "", err
	}
	return url.PathEscape(id), nil
}

func validateTemplateResponse(template *Template) error {
	return identifier.ValidateRunPodID(template.ID)
}

func validateEndpointResponse(endpoint *Endpoint) error {
	if err := identifier.ValidateRunPodID(endpoint.ID); err != nil {
		return err
	}
	if err := identifier.ValidateRunPodID(endpoint.TemplateID); err != nil {
		return errors.New("RunPod Endpoint response omitted or returned an invalid templateId")
	}
	return nil
}

func validateEndpointObservation(endpoint *Endpoint) error {
	if err := validateEndpointResponse(endpoint); err != nil {
		return err
	}
	if endpoint.WorkersMin == nil || endpoint.WorkersMax == nil || endpoint.Version == nil {
		return errors.New("RunPod Endpoint response omitted workersMin, workersMax, or version")
	}
	if endpoint.Workers == nil {
		return errors.New("RunPod Endpoint response omitted workers after includeWorkers=true")
	}
	if endpoint.Template == nil || endpoint.Template.ID != endpoint.TemplateID {
		return errors.New("RunPod Endpoint response omitted or mismatched the bound Template after includeTemplate=true")
	}
	if err := validateTemplateResponse(endpoint.Template); err != nil {
		return errors.New("RunPod Endpoint response returned an invalid bound Template ID")
	}
	for i := range endpoint.Workers {
		worker := &endpoint.Workers[i]
		if !worker.ID.Present || worker.ID.Null || identifier.ValidateRunPodID(worker.ID.Value) != nil ||
			!worker.EndpointID.Present || (!worker.EndpointID.Null && worker.EndpointID.Value != endpoint.ID) ||
			!worker.TemplateID.Present || (!worker.TemplateID.Null && worker.TemplateID.Value != endpoint.TemplateID) ||
			worker.SLSVersion == nil {
			return errors.New("RunPod Endpoint worker response omitted or returned an invalid identity binding")
		}
	}
	return nil
}

func normalizeTemplateInput(in TemplateInput) TemplateInput {
	if in.Category == "" {
		in.Category = "NVIDIA"
	}
	// RunPod defaults an omitted container disk to 50 GiB. Serialize the same
	// bounded ephemeral value so an API-default change cannot alter the worker.
	if in.ContainerDiskInGB == 0 {
		in.ContainerDiskInGB = 50
	}
	if in.DockerEntrypoint == nil {
		in.DockerEntrypoint = []string{}
	}
	if in.DockerStartCmd == nil {
		in.DockerStartCmd = []string{}
	}
	if in.Env == nil {
		in.Env = map[string]string{}
	}
	return in
}

func templateUpdateInput(in TemplateInput) TemplateUpdateInput {
	in = normalizeTemplateInput(in)
	return TemplateUpdateInput{
		Name: in.Name, ImageName: in.ImageName, IsPublic: in.IsPublic,
		ContainerDiskInGB: in.ContainerDiskInGB, DockerEntrypoint: in.DockerEntrypoint,
		DockerStartCmd: in.DockerStartCmd, Ports: in.Ports, Readme: in.Readme,
		VolumeInGB: in.VolumeInGB, VolumeMountPath: in.VolumeMountPath,
		Env: in.Env, ContainerRegistryAuthID: in.ContainerRegistryAuthID,
	}
}

func normalizeEndpointInput(in EndpointInput) EndpointInput {
	if in.AllowedCUDAVersions == nil {
		in.AllowedCUDAVersions = []string{}
	}
	if in.NetworkVolumeIDs == nil {
		in.NetworkVolumeIDs = []string{}
	}
	return in
}

func endpointUpdateInput(in EndpointInput) EndpointUpdateInput {
	in = normalizeEndpointInput(in)
	return EndpointUpdateInput{
		Name: in.Name, TemplateID: in.TemplateID, GPUCount: in.GPUCount,
		GPUTypeIDs: in.GPUTypeIDs, AllowedCUDAVersions: in.AllowedCUDAVersions,
		DataCenterIDs: in.DataCenterIDs, WorkersMin: in.WorkersMin, WorkersMax: in.WorkersMax,
		IdleTimeout: in.IdleTimeout, ScalerType: in.ScalerType, ScalerValue: in.ScalerValue,
		ExecutionTimeoutMS: in.ExecutionTimeoutMS, FlashBoot: in.FlashBoot,
		NetworkVolumeID: in.NetworkVolumeID, NetworkVolumeIDs: in.NetworkVolumeIDs,
		MinCUDAVersion: in.MinCUDAVersion,
	}
}

func (c *Client) doJSON(ctx context.Context, method, path string, input, output any) error {
	var payload []byte
	var err error
	if input != nil {
		payload, err = json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode RunPod request: %w", err)
		}
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, reqErr := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.baseURL.String(), "/")+path, bytes.NewReader(payload))
		if reqErr != nil {
			return fmt.Errorf("create RunPod request: %w", reqErr)
		}
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "provider-runpod/clean-room")
		if input != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, doErr := c.httpClient.Do(req)
		if doErr != nil {
			// A failed POST is ambiguous. Surface it immediately; callers recover
			// through an exact-name lookup instead of issuing a second chargeable POST.
			if method == http.MethodPost {
				return fmt.Errorf("%w: %v", ErrCreateAmbiguous, doErr)
			}
			// PATCH triggers a RunPod rollout and DELETE changes lifecycle state.
			// Neither is retried inside one call when the response is uncertain;
			// the next Crossplane Observe must re-establish external state first.
			if method != http.MethodGet || attempt == maxAttempts-1 {
				return fmt.Errorf("call RunPod API: %w", doErr)
			}
			if err := c.sleep(ctx, backoff(attempt, "")); err != nil {
				return err
			}
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
		closeErr := resp.Body.Close()
		if readErr != nil {
			return postAmbiguous(method, fmt.Errorf("read RunPod response: %w", readErr))
		}
		if closeErr != nil {
			return postAmbiguous(method, fmt.Errorf("close RunPod response: %w", closeErr))
		}
		if len(body) > maxBodyBytes {
			return postAmbiguous(method, errors.New("RunPod response exceeded 1 MiB limit"))
		}

		if resp.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if retryable(resp.StatusCode) && attempt < maxAttempts-1 {
			// POST may have committed even when the service returned an error.
			// Never repeat a potentially chargeable create; the controller uses
			// an exact, provider-owned name to recover the result instead.
			if method == http.MethodPost {
				return fmt.Errorf("%w: %w", ErrCreateAmbiguous, &APIError{StatusCode: resp.StatusCode, RequestID: requestID(resp.Header)})
			}
			if method != http.MethodGet {
				return &APIError{StatusCode: resp.StatusCode, RequestID: requestID(resp.Header)}
			}
			if err := c.sleep(ctx, backoff(attempt, resp.Header.Get("Retry-After"))); err != nil {
				return err
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return &APIError{StatusCode: resp.StatusCode, RequestID: requestID(resp.Header)}
		}
		if output == nil || len(bytes.TrimSpace(body)) == 0 {
			return nil
		}
		if err := json.Unmarshal(body, output); err != nil {
			return postAmbiguous(method, fmt.Errorf("decode RunPod response: %w", err))
		}
		return nil
	}
	return errors.New("RunPod retry budget exhausted")
}

func postAmbiguous(method string, err error) error {
	if method == http.MethodPost {
		// The request was already on the wire and the response cannot establish
		// whether creation committed. Force deterministic name-based recovery;
		// the controller must never turn these failures into a second POST.
		return fmt.Errorf("%w: %v", ErrCreateAmbiguous, err)
	}
	return err
}

func retryable(status int) bool { return status == http.StatusTooManyRequests || status >= 500 }

func backoff(attempt int, retryAfter string) time.Duration {
	if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds > 0 {
		d := time.Duration(seconds) * time.Second
		if d > 10*time.Second {
			return 10 * time.Second
		}
		return d
	}
	return time.Duration(1<<attempt) * 100 * time.Millisecond
}

func requestID(h http.Header) string {
	for _, key := range []string{"X-Request-Id", "X-Request-ID", "Request-Id"} {
		if v := h.Get(key); safeRequestID.MatchString(v) {
			return v
		}
	}
	return ""
}
