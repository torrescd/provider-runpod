// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package runpod

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTemplateLifecycleUsesOfficialContractAndRedactsErrors(t *testing.T) {
	token := "rpa_super-secret-value"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("missing bearer credential")
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/templates":
			body, _ := io.ReadAll(r.Body)
			if strings.Contains(string(body), token) || !strings.Contains(string(body), `"isServerless":true`) || !strings.Contains(string(body), `"isPublic":false`) {
				t.Errorf("unsafe create body: %s", body)
			}
			fields := map[string]json.RawMessage{}
			if err := json.Unmarshal(body, &fields); err != nil {
				t.Errorf("decode create body: %v", err)
			}
			var volume int32
			rawVolume, present := fields["volumeInGb"]
			if !present || json.Unmarshal(rawVolume, &volume) != nil || volume != 0 {
				t.Errorf("volumeInGb must be explicitly serialized as zero: %s", body)
			}
			var mount string
			if raw, present := fields["volumeMountPath"]; !present || json.Unmarshal(raw, &mount) != nil || mount != "" {
				t.Errorf("volumeMountPath must be explicitly cleared: %s", body)
			}
			var readme string
			if raw, present := fields["readme"]; !present || json.Unmarshal(raw, &readme) != nil || readme != "" {
				t.Errorf("readme must be explicitly cleared: %s", body)
			}
			var containerDisk int32
			if raw, present := fields["containerDiskInGb"]; !present || json.Unmarshal(raw, &containerDisk) != nil || containerDisk != 50 {
				t.Errorf("containerDiskInGb must be explicitly serialized as 50: %s", body)
			}
			var category string
			if raw, present := fields["category"]; !present || json.Unmarshal(raw, &category) != nil || category != "NVIDIA" {
				t.Errorf("Template category must be explicitly NVIDIA: %s", body)
			}
			var env map[string]string
			if raw, present := fields["env"]; !present || json.Unmarshal(raw, &env) != nil || len(env) != 0 {
				t.Errorf("Template env must be explicitly cleared: %s", body)
			}
			var registryAuth string
			if raw, present := fields["containerRegistryAuthId"]; !present || json.Unmarshal(raw, &registryAuth) != nil || registryAuth != "" {
				t.Errorf("Template registry auth must be explicitly cleared: %s", body)
			}
			for _, field := range []string{"dockerEntrypoint", "dockerStartCmd"} {
				var command []string
				if raw, present := fields[field]; !present || json.Unmarshal(raw, &command) != nil || len(command) != 0 {
					t.Errorf("%s must be explicitly cleared: %s", field, body)
				}
			}
			var ports []string
			if json.Unmarshal(fields["ports"], &ports) != nil || strings.Join(ports, ",") != "8000/http" {
				t.Errorf("explicit bounded ports were not serialized: %s", body)
			}
			_, _ = io.WriteString(w, `{"id":"tpl_1","name":"experiment","category":"NVIDIA","imageName":"registry/model@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","isPublic":false,"isServerless":true}`)
		case r.Method == http.MethodPatch && r.URL.Path == "/templates/tpl_1":
			fields := map[string]json.RawMessage{}
			if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
				t.Fatal(err)
			}
			if _, present := fields["category"]; present {
				t.Fatal("Template PATCH included create-only category")
			}
			if _, present := fields["isServerless"]; present {
				t.Fatal("Template PATCH included create-only isServerless")
			}
			_, _ = io.WriteString(w, `{"id":"tpl_1","name":"experiment","category":"NVIDIA","imageName":"registry/model@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","isPublic":false,"isServerless":true}`)
		case r.Method == http.MethodGet && r.URL.Path == "/templates/tpl_1":
			if r.URL.Query().Get("includeEndpointBoundTemplates") != "true" {
				t.Errorf("bound Template lookup omitted includeEndpointBoundTemplates=true: %s", r.URL.RawQuery)
			}
			w.Header().Set("X-Request-ID", "req-safe")
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"secret":"`+token+`"}`)
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	c, err := New([]byte(token), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateTemplate(context.Background(), TemplateInput{Name: "experiment", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), IsServerless: true, Ports: []string{"8000/http"}})
	if err != nil || created.ID != "tpl_1" {
		t.Fatalf("create: %#v %v", created, err)
	}
	if _, err := c.UpdateTemplate(context.Background(), "tpl_1", TemplateInput{Name: "experiment", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), IsServerless: true, Ports: []string{"8000/http"}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	_, err = c.GetTemplate(context.Background(), "tpl_1")
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "secret") || !strings.Contains(err.Error(), "req-safe") {
		t.Fatalf("error was not safely summarized: %v", err)
	}
	if requests != 3 {
		t.Fatalf("requests = %d", requests)
	}
}

func TestEndpointDataCenterIDsAcceptBothOfficialEncodings(t *testing.T) {
	for name, body := range map[string]string{
		"OpenAPI array":            `{"id":"endpoint_1","dataCenterIds":["EU-RO-1","US-KS-2"]}`,
		"published example string": `{"id":"endpoint_1","dataCenterIds":" EU-RO-1, US-KS-2 "}`,
	} {
		t.Run(name, func(t *testing.T) {
			var endpoint Endpoint
			if err := json.Unmarshal([]byte(body), &endpoint); err != nil {
				t.Fatal(err)
			}
			if strings.Join(endpoint.DataCenterIDs, ",") != "EU-RO-1,US-KS-2" {
				t.Fatalf("normalized dataCenterIds=%v", endpoint.DataCenterIDs)
			}
		})
	}
}

func TestEndpointDataCenterIDsRejectMalformedEncodings(t *testing.T) {
	for name, body := range map[string]string{
		"empty segment":      `{"dataCenterIds":"EU-RO-1,,US-KS-2"}`,
		"non-string element": `{"dataCenterIds":["EU-RO-1",2]}`,
		"wrong scalar":       `{"dataCenterIds":42}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(body), &Endpoint{}); err == nil {
				t.Fatal("malformed dataCenterIds was accepted")
			}
		})
	}
}

func TestWorkerDecimalFieldsAcceptOfficialStringAndNumberEncodings(t *testing.T) {
	var worker EndpointWorker
	body := `{"costPerHr":"0.74","adjustedCostPerHr":0.75,"machine":{"costPerHr":"0.72","currentPricePerGpu":"0.73"}}`
	if err := json.Unmarshal([]byte(body), &worker); err != nil {
		t.Fatal(err)
	}
	if !worker.CostPerHr.Present || worker.CostPerHr.Null || worker.CostPerHr.Value != 0.74 ||
		!worker.AdjustedCostPerHr.Present || worker.AdjustedCostPerHr.Value != 0.75 || worker.Machine == nil ||
		!worker.Machine.CostPerHr.Present || worker.Machine.CostPerHr.Value != 0.72 ||
		!worker.Machine.CurrentPricePerGPU.Present || worker.Machine.CurrentPricePerGPU.Value != 0.73 {
		t.Fatalf("official worker cost fields were not decoded exactly: %+v", worker)
	}
	for name, body := range map[string]string{
		"malformed": `{"costPerHr":"not-a-decimal"}`,
		"infinite":  `{"costPerHr":"Inf"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(body), &EndpointWorker{}); err == nil {
				t.Fatal("unsafe worker cost encoding was accepted")
			}
		})
	}
}

func TestEndpointObservationRequiresPresenceAndExactIncludeQueries(t *testing.T) {
	valid := `{"id":"endpoint_1","name":"owned","templateId":"tpl_1","workersMin":0,"workersMax":1,"version":0,"workers":[],"template":{"id":"tpl_1"}}`
	for name, body := range map[string]string{
		"valid zero version":         valid,
		"missing template ID":        `{"id":"endpoint_1","workersMin":0,"workersMax":1,"version":0,"workers":[],"template":{"id":"tpl_1"}}`,
		"missing workers minimum":    `{"id":"endpoint_1","templateId":"tpl_1","workersMax":1,"version":0,"workers":[],"template":{"id":"tpl_1"}}`,
		"null workers minimum":       `{"id":"endpoint_1","templateId":"tpl_1","workersMin":null,"workersMax":1,"version":0,"workers":[],"template":{"id":"tpl_1"}}`,
		"missing workers maximum":    `{"id":"endpoint_1","templateId":"tpl_1","workersMin":0,"version":0,"workers":[],"template":{"id":"tpl_1"}}`,
		"missing version":            `{"id":"endpoint_1","templateId":"tpl_1","workersMin":0,"workersMax":1,"workers":[],"template":{"id":"tpl_1"}}`,
		"null version":               `{"id":"endpoint_1","templateId":"tpl_1","workersMin":0,"workersMax":1,"version":null,"workers":[],"template":{"id":"tpl_1"}}`,
		"missing workers collection": `{"id":"endpoint_1","templateId":"tpl_1","workersMin":0,"workersMax":1,"version":0,"template":{"id":"tpl_1"}}`,
		"null workers collection":    `{"id":"endpoint_1","templateId":"tpl_1","workersMin":0,"workersMax":1,"version":0,"workers":null,"template":{"id":"tpl_1"}}`,
		"missing nested template":    `{"id":"endpoint_1","templateId":"tpl_1","workersMin":0,"workersMax":1,"version":0,"workers":[]}`,
		"mismatched nested template": `{"id":"endpoint_1","templateId":"tpl_1","workersMin":0,"workersMax":1,"version":0,"workers":[],"template":{"id":"tpl_other"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("includeTemplate") != "true" || r.URL.Query().Get("includeWorkers") != "true" {
					t.Fatalf("bounded observation query=%q", r.URL.RawQuery)
				}
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			c, err := New([]byte("management-key"), WithBaseURLForTesting(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.GetEndpoint(context.Background(), "endpoint_1")
			if name == "valid zero version" && err != nil {
				t.Fatalf("valid documented version zero rejected: %v", err)
			}
			if name != "valid zero version" && err == nil {
				t.Fatal("omitted, null, or mismatched security observation was accepted")
			}
		})
	}
}

func TestCreateAndUpdateResponsesMustMatchRequestedIdentity(t *testing.T) {
	endpointBody := func(id, name, templateID string) string {
		return `{"id":"` + id + `","name":"` + name + `","templateId":"` + templateID + `","workersMin":0,"workersMax":1,"version":0,"workers":[],"template":{"id":"` + templateID + `"}}`
	}
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
		invoke func(*Client) error
		create bool
	}{
		{name: "Template create omitted name", method: http.MethodPost, path: "/templates", body: `{"id":"tpl_1"}`, create: true, invoke: func(c *Client) error {
			_, err := c.CreateTemplate(context.Background(), TemplateInput{Name: "owned"})
			return err
		}},
		{name: "Template create invalid ID", method: http.MethodPost, path: "/templates", body: `{"id":"../pods/x","name":"owned"}`, create: true, invoke: func(c *Client) error {
			_, err := c.CreateTemplate(context.Background(), TemplateInput{Name: "owned"})
			return err
		}},
		{name: "Template update wrong name", method: http.MethodPatch, path: "/templates/tpl_1", body: `{"id":"tpl_1","name":"other"}`, invoke: func(c *Client) error {
			_, err := c.UpdateTemplate(context.Background(), "tpl_1", TemplateInput{Name: "owned"})
			return err
		}},
		{name: "Template update wrong ID", method: http.MethodPatch, path: "/templates/tpl_1", body: `{"id":"tpl_other","name":"owned"}`, invoke: func(c *Client) error {
			_, err := c.UpdateTemplate(context.Background(), "tpl_1", TemplateInput{Name: "owned"})
			return err
		}},
		{name: "Endpoint create wrong name", method: http.MethodPost, path: "/endpoints", body: endpointBody("endpoint_1", "other", "tpl_1"), create: true, invoke: func(c *Client) error {
			_, err := c.CreateEndpoint(context.Background(), EndpointInput{Name: "owned", TemplateID: "tpl_1"})
			return err
		}},
		{name: "Endpoint create wrong template", method: http.MethodPost, path: "/endpoints", body: endpointBody("endpoint_1", "owned", "tpl_other"), create: true, invoke: func(c *Client) error {
			_, err := c.CreateEndpoint(context.Background(), EndpointInput{Name: "owned", TemplateID: "tpl_1"})
			return err
		}},
		{name: "Endpoint update wrong name", method: http.MethodPatch, path: "/endpoints/endpoint_1", body: endpointBody("endpoint_1", "other", "tpl_1"), invoke: func(c *Client) error {
			_, err := c.UpdateEndpoint(context.Background(), "endpoint_1", EndpointInput{Name: "owned", TemplateID: "tpl_1"})
			return err
		}},
		{name: "Endpoint update wrong ID", method: http.MethodPatch, path: "/endpoints/endpoint_1", body: endpointBody("endpoint_other", "owned", "tpl_1"), invoke: func(c *Client) error {
			_, err := c.UpdateEndpoint(context.Background(), "endpoint_1", EndpointInput{Name: "owned", TemplateID: "tpl_1"})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != tc.method || r.URL.Path != tc.path {
					t.Fatalf("request=%s %s", r.Method, r.URL.Path)
				}
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			c, err := New([]byte("management-key"), WithBaseURLForTesting(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			err = tc.invoke(c)
			if err == nil {
				t.Fatal("mismatched mutation response was accepted")
			}
			if tc.create && !errors.Is(err, ErrCreateAmbiguous) {
				t.Fatalf("create identity mismatch=%v, want ErrCreateAmbiguous", err)
			}
		})
	}
}

func TestEndpointCreateAndUpdateUseExactOfficialWireSubsets(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if (r.Method != http.MethodPost || r.URL.Path != "/endpoints") && (r.Method != http.MethodPatch || r.URL.Path != "/endpoints/endpoint_1") {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		fields := map[string]json.RawMessage{}
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			t.Fatal(err)
		}
		var flashBoot bool
		raw, present := fields["flashboot"]
		if !present || json.Unmarshal(raw, &flashBoot) != nil || flashBoot {
			t.Fatalf("flashboot must be explicitly serialized as false: %s", raw)
		}
		if raw, present := fields["networkVolumeId"]; present {
			t.Fatalf("unset networkVolumeId has no documented clear sentinel and must be omitted: %s", raw)
		}
		if raw, present := fields["networkVolumeIds"]; present {
			t.Fatalf("unset networkVolumeIds has no documented clear sentinel and must be omitted: %s", raw)
		}
		if raw, present := fields["allowedCudaVersions"]; present {
			t.Fatalf("unset allowedCudaVersions must be omitted rather than sent as an undocumented empty sentinel: %s", raw)
		}
		if raw, present := fields["minCudaVersion"]; present {
			t.Fatalf("unset minCudaVersion is outside the official enum and must be omitted: %s", raw)
		}
		_, hasComputeType := fields["computeType"]
		if r.Method == http.MethodPost && !hasComputeType {
			t.Fatal("Endpoint POST omitted create-only computeType")
		}
		if r.Method == http.MethodPatch && hasComputeType {
			t.Fatal("Endpoint PATCH included create-only computeType")
		}
		for _, field := range []string{"templateId", "gpuCount", "gpuTypeIds", "dataCenterIds", "workersMin", "workersMax", "idleTimeout", "scalerType", "scalerValue", "executionTimeoutMs"} {
			if _, present := fields[field]; !present {
				t.Fatalf("%s omitted required bounded field %s: %#v", r.Method, field, fields)
			}
		}
		_, _ = io.WriteString(w, `{"id":"endpoint_1","name":"","templateId":"tpl_1","workersMin":0,"workersMax":0,"version":0,"workers":[],"template":{"id":"tpl_1"}}`)
	}))
	defer server.Close()
	c, err := New([]byte("management-key"), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	input := EndpointInput{TemplateID: "tpl_1", ComputeType: "GPU", GPUTypeIDs: []string{"NVIDIA L4"}, DataCenterIDs: []string{"EU-RO-1"}}
	if _, err := c.CreateEndpoint(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateEndpoint(context.Background(), "endpoint_1", input); err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("wire requests=%d, want POST and PATCH", requests)
	}
}

func TestCreateTransportFailureIsNotRetried(t *testing.T) {
	rt := roundTripFunc{fn: func(*http.Request) (*http.Response, error) { return nil, errors.New("network down with no response") }}
	h := &http.Client{Transport: &rt, Timeout: time.Second}
	c, err := New([]byte("safe-key"), WithHTTPClient(h))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateEndpoint(context.Background(), EndpointInput{Name: "unique", TemplateID: "tpl_1"})
	if !errors.Is(err, ErrCreateAmbiguous) || rt.calls != 1 {
		t.Fatalf("err=%v calls=%d", err, rt.calls)
	}
}

func TestCreateHTTPFailureIsNotRetried(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("X-Request-ID", "safe-request-id")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	c, err := New([]byte("safe-key"), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateTemplate(context.Background(), TemplateInput{Name: "unique"})
	if !errors.Is(err, ErrCreateAmbiguous) || calls != 1 || !strings.Contains(err.Error(), "safe-request-id") {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestCreateUncertainSuccessResponsesAreAmbiguousAndNeverRetried(t *testing.T) {
	for name, body := range map[string]string{
		"malformed JSON": `{`,
		"oversize body":  strings.Repeat("x", maxBodyBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			c, err := New([]byte("safe-key"), WithBaseURLForTesting(server.URL))
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.CreateTemplate(context.Background(), TemplateInput{Name: "unique"})
			if !errors.Is(err, ErrCreateAmbiguous) || calls != 1 {
				t.Fatalf("err=%v calls=%d", err, calls)
			}
		})
	}

	t.Run("truncated body", func(t *testing.T) {
		rt := roundTripFunc{fn: func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusCreated,
				Header:     make(http.Header),
				Body:       io.NopCloser(io.MultiReader(strings.NewReader(`{"id":"endpoint_1"`), errReader{})),
			}, nil
		}}
		c, err := New([]byte("safe-key"), WithHTTPClient(&http.Client{Transport: &rt}))
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.CreateEndpoint(context.Background(), EndpointInput{Name: "unique", TemplateID: "tpl_1"})
		if !errors.Is(err, ErrCreateAmbiguous) || rt.calls != 1 {
			t.Fatalf("err=%v calls=%d", err, rt.calls)
		}
	})
}

func TestEndpointDeleteDoesNotRetryUncertainScaleToZeroPatch(t *testing.T) {
	calls := 0
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		methods = append(methods, r.Method+" "+r.URL.Path)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	c, err := New([]byte("safe-key"), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	c.sleep = func(context.Context, time.Duration) error { return nil }
	if err := c.DeleteEndpoint(context.Background(), "endpoint_1"); err == nil {
		t.Fatal("retryable PATCH response was hidden by an in-call rollout retry")
	}
	want := []string{"PATCH /endpoints/endpoint_1"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("methods=%v", methods)
	}
}

func TestCommittedPatchWithLostResponseIsNeverRetried(t *testing.T) {
	rt := roundTripFunc{fn: func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPatch {
			t.Fatalf("method=%s, want PATCH", req.Method)
		}
		return nil, io.ErrUnexpectedEOF
	}}
	c, err := New([]byte("safe-key"), WithHTTPClient(&http.Client{Transport: &rt}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateEndpoint(context.Background(), "endpoint_1", EndpointInput{TemplateID: "tpl_1"}); err == nil {
		t.Fatal("uncertain PATCH unexpectedly succeeded")
	}
	if rt.calls != 1 {
		t.Fatalf("uncertain PATCH calls=%d, want exactly one before Observe", rt.calls)
	}
}

func TestSafeGETStillRetriesTransientResponse(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	defer server.Close()
	c, err := New([]byte("safe-key"), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	c.sleep = func(context.Context, time.Duration) error { return nil }
	if _, err := c.ListEndpoints(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("GET calls=%d, want one safe retry", calls)
	}
}

func TestProductionTransportHonorsProxyEnvironment(t *testing.T) {
	c, err := New([]byte("safe-key"))
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := c.httpClient.Transport.(*http.Transport)
	if !ok || transport.Proxy == nil {
		t.Fatal("transport has no environment proxy function")
	}
}

func TestManagementCredentialNeverFollowsRedirect(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected = true
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	c, err := New([]byte("management-key"), WithBaseURLForTesting(source.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetTemplate(context.Background(), "template_1")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
		t.Fatalf("redirect response=%v, want sanitized HTTP 302", err)
	}
	if redirected {
		t.Fatal("management credential followed redirect to a second origin")
	}
}

func TestUnsafeExternalNamesNeverReachManagementHTTP(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests++
	}))
	defer server.Close()
	c, err := New([]byte("management-key"), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{".", "..", "../pods/x", "a/b", "a%2Fb", "a?x", "a#x", "a\nb"} {
		operations := []func() error{
			func() error { _, err := c.GetTemplate(context.Background(), id); return err },
			func() error { _, err := c.UpdateTemplate(context.Background(), id, TemplateInput{}); return err },
			func() error { return c.DeleteTemplate(context.Background(), id) },
			func() error { _, err := c.GetEndpoint(context.Background(), id); return err },
			func() error {
				_, err := c.UpdateEndpoint(context.Background(), id, EndpointInput{TemplateID: "tpl_1"})
				return err
			},
			func() error { return c.DeleteEndpoint(context.Background(), id) },
		}
		for _, operation := range operations {
			if err := operation(); err == nil {
				t.Fatalf("unsafe external name %q was accepted", id)
			}
		}
	}
	if _, err := c.CreateEndpoint(context.Background(), EndpointInput{TemplateID: "../templates/other"}); err == nil {
		t.Fatal("unsafe direct templateId was accepted")
	}
	if requests != 0 {
		t.Fatalf("unsafe IDs caused %d management HTTP requests", requests)
	}
}

func TestMalformedListDerivedIDsFailClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/templates":
			_, _ = io.WriteString(w, `[{"id":"../pods/escape","name":"owned"}]`)
		case "/endpoints":
			_, _ = io.WriteString(w, `[{"id":"endpoint_1","templateId":"../templates/escape"}]`)
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	c, err := New([]byte("management-key"), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.FindTemplateByName(context.Background(), "owned"); err == nil {
		t.Fatal("malformed recovery-derived Template ID was accepted")
	}
	if _, err := c.ListEndpoints(context.Background()); err == nil {
		t.Fatal("malformed list-derived Template ID was accepted")
	}
}

func TestManagementKeyRejectsSurroundingWhitespace(t *testing.T) {
	for _, key := range []string{" management-key", "management-key ", "\tmanagement-key"} {
		if _, err := New([]byte(key)); err == nil {
			t.Fatalf("non-canonical management key %q was accepted", key)
		}
	}
}

func TestRequestIDSafelyBoundsUntrustedResponseMetadata(t *testing.T) {
	for name, value := range map[string]string{
		"too long": strings.Repeat("a", 129),
		"control":  "safe\nforged-event",
		"spaces":   "not an identifier",
		"unicode":  "request-☃",
	} {
		t.Run(name, func(t *testing.T) {
			if got := requestID(http.Header{"X-Request-Id": []string{value}}); got != "" {
				t.Fatalf("unsafe request ID was retained: %q", got)
			}
		})
	}
	if got := requestID(http.Header{"X-Request-Id": []string{"req_123:attempt-2"}}); got != "req_123:attempt-2" {
		t.Fatalf("safe request ID=%q", got)
	}
}

type roundTripFunc struct {
	fn    func(*http.Request) (*http.Response, error)
	calls int
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func (r *roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	return r.fn(req)
}
