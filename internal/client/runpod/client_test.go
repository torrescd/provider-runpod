// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package runpod

import (
	"context"
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
			_, _ = io.WriteString(w, `{"id":"tpl_1","name":"experiment","imageName":"registry/model@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","isPublic":false,"isServerless":true}`)
		case r.Method == http.MethodGet && r.URL.Path == "/templates/tpl_1":
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
	created, err := c.CreateTemplate(context.Background(), TemplateInput{Name: "experiment", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), IsServerless: true})
	if err != nil || created.ID != "tpl_1" {
		t.Fatalf("create: %#v %v", created, err)
	}
	_, err = c.GetTemplate(context.Background(), "tpl_1")
	if err == nil || strings.Contains(err.Error(), token) || strings.Contains(err.Error(), "secret") || !strings.Contains(err.Error(), "req-safe") {
		t.Fatalf("error was not safely summarized: %v", err)
	}
	if requests != 2 {
		t.Fatalf("requests = %d", requests)
	}
}

func TestCreateTransportFailureIsNotRetried(t *testing.T) {
	rt := roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("network down with no response") })
	h := &http.Client{Transport: &rt, Timeout: time.Second}
	c, err := New([]byte("safe-key"), WithHTTPClient(h))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CreateEndpoint(context.Background(), EndpointInput{Name: "unique"})
	if !errors.Is(err, ErrCreateAmbiguous) || rt.calls != 1 {
		t.Fatalf("err=%v calls=%d", err, rt.calls)
	}
}

func TestRetries429AndEndpointDeleteScalesToZero(t *testing.T) {
	calls := 0
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		methods = append(methods, r.Method+" "+r.URL.Path)
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	c, err := New([]byte("safe-key"), WithBaseURLForTesting(server.URL))
	if err != nil {
		t.Fatal(err)
	}
	c.sleep = func(context.Context, time.Duration) error { return nil }
	if err := c.DeleteEndpoint(context.Background(), "endpoint_1"); err != nil {
		t.Fatal(err)
	}
	want := []string{"PATCH /endpoints/endpoint_1", "PATCH /endpoints/endpoint_1", "DELETE /endpoints/endpoint_1"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("methods=%v", methods)
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

type roundTripFunc struct {
	fn    func(*http.Request) (*http.Response, error)
	calls int
}

func (r *roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	return r.fn(req)
}
