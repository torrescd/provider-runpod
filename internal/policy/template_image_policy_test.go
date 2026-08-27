// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package policy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestTemplateImagePolicyContractFailsClosed(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate policy test")
	}
	path := filepath.Join(filepath.Dir(source), "..", "..", "examples", "policy", "template-image-verification.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	jsonPolicy, err := utilyaml.ToJSON(raw)
	if err != nil {
		t.Fatalf("policy is not valid YAML: %v", err)
	}
	var policy map[string]any
	if err := json.Unmarshal(jsonPolicy, &policy); err != nil {
		t.Fatal(err)
	}
	if policy["apiVersion"] != "policies.kyverno.io/v1beta1" || policy["kind"] != "ImageValidatingPolicy" {
		t.Fatalf("unexpected policy type: %v %v", policy["apiVersion"], policy["kind"])
	}

	text := string(raw)
	required := []string{
		"failurePolicy: Fail",
		"- Deny",
		"serverless.runpod.crossplane.io",
		"- templates",
		`expression: "[object.spec.forProvider.imageName]"`,
		`glob: "*"`,
		"mutateDigest: false",
		"verifyDigest: true",
		"required: true",
		"issuer: https://token.actions.githubusercontent.com",
		"subject: urn:provider-runpod:unconfigured-deny-all",
		"type: https://slsa.dev/provenance/v1",
		"type: https://spdx.dev/Document",
		"verifyImageSignatures",
		"verifyAttestationSignatures",
	}
	for _, fragment := range required {
		if !strings.Contains(text, fragment) {
			t.Errorf("fail-closed policy contract lost %q", fragment)
		}
	}
	if strings.Contains(text, "failurePolicy: Ignore") || strings.Contains(text, "validationActions: [Audit]") {
		t.Fatal("policy contains a fail-open setting")
	}
}
