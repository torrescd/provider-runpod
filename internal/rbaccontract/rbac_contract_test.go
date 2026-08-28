// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package rbaccontract

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

type manifest struct {
	Kind     string              `json:"kind"`
	Metadata objectMeta          `json:"metadata"`
	Rules    []rbacv1.PolicyRule `json:"rules"`
	Subjects []rbacv1.Subject    `json:"subjects"`
	RoleRef  rbacv1.RoleRef      `json:"roleRef"`
}

type objectMeta struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

func TestProviderRBACContract(t *testing.T) {
	docs := decodeManifests(t, "examples", "rbac", "provider-rbac-manager-disabled.yaml")
	serviceAccount := findManifest(t, docs, "ServiceAccount", "provider-runpod")
	if serviceAccount.Metadata.Namespace != "crossplane-system" {
		t.Fatalf("provider ServiceAccount namespace=%q", serviceAccount.Metadata.Namespace)
	}

	clusterRole := findManifest(t, docs, "ClusterRole", "provider-runpod")
	if got := rulesForResource(clusterRole.Rules, "secrets"); len(got) != 0 {
		t.Fatalf("provider ClusterRole grants Secret access: %#v", got)
	}
	assertRule(t, clusterRole.Rules, rbacv1.PolicyRule{
		APIGroups: []string{"verification.runpod.crossplane.io"},
		Resources: []string{"endpointchecks"},
		Verbs:     []string{"get", "list", "delete"},
	})
	if got := rulesForResource(clusterRole.Rules, "endpointchecks/status"); len(got) != 0 {
		t.Fatalf("provider can mutate EndpointCheck status: %#v", got)
	}

	binding := findManifest(t, docs, "ClusterRoleBinding", "provider-runpod")
	assertBinding(t, binding, "ClusterRole", "provider-runpod", "provider-runpod", "crossplane-system")

	secretRole := findManifest(t, docs, "Role", "provider-runpod-management-credential")
	if secretRole.Metadata.Namespace != "runpod-credentials" {
		t.Fatalf("management Secret Role namespace=%q", secretRole.Metadata.Namespace)
	}
	assertOnlySecretRule(t, secretRole.Rules, "runpod-management")
	secretBinding := findManifest(t, docs, "RoleBinding", "provider-runpod-management-credential")
	assertBinding(t, secretBinding, "Role", "provider-runpod-management-credential", "provider-runpod", "crossplane-system")
	if secretBinding.Metadata.Namespace != "runpod-credentials" {
		t.Fatalf("management Secret RoleBinding namespace=%q", secretBinding.Metadata.Namespace)
	}
}

func TestModelRouterRBACContract(t *testing.T) {
	docs := decodeManifests(t, "examples", "router", "model-router.yaml")
	serviceAccount := findManifest(t, docs, "ServiceAccount", "provider-runpod-model-router")
	if serviceAccount.Metadata.Namespace != "runpod-system" {
		t.Fatalf("router ServiceAccount namespace=%q", serviceAccount.Metadata.Namespace)
	}

	role := findManifest(t, docs, "Role", "provider-runpod-model-router")
	if role.Metadata.Namespace != "runpod-system" {
		t.Fatalf("router Role namespace=%q", role.Metadata.Namespace)
	}
	for _, rule := range role.Rules {
		for _, group := range rule.APIGroups {
			if group == "*" || group == "runpod.crossplane.io" {
				t.Fatalf("router Role grants ProviderConfig API access: %#v", rule)
			}
		}
	}
	assertOnlySecretRule(t, role.Rules, "runpod-inference")
	assertRule(t, role.Rules, rbacv1.PolicyRule{
		APIGroups: []string{"verification.runpod.crossplane.io"},
		Resources: []string{"endpointchecks"},
		Verbs:     []string{"get", "list", "watch", "update", "delete"},
	})
	assertRule(t, role.Rules, rbacv1.PolicyRule{
		APIGroups:     []string{"serverless.runpod.crossplane.io"},
		Resources:     []string{"templates"},
		ResourceNames: []string{"opencode-model"},
		Verbs:         []string{"get"},
	})
	assertRule(t, role.Rules, rbacv1.PolicyRule{
		APIGroups: []string{"verification.runpod.crossplane.io"},
		Resources: []string{"endpointchecks/status"},
		Verbs:     []string{"update"},
	})
	assertRule(t, role.Rules, rbacv1.PolicyRule{
		APIGroups:     []string{"serverless.runpod.crossplane.io"},
		Resources:     []string{"endpoints"},
		ResourceNames: []string{"opencode-model"},
		Verbs:         []string{"get", "patch"},
	})

	binding := findManifest(t, docs, "RoleBinding", "provider-runpod-model-router")
	assertBinding(t, binding, "Role", "provider-runpod-model-router", "provider-runpod-model-router", "runpod-system")
}

func assertOnlySecretRule(t *testing.T, rules []rbacv1.PolicyRule, name string) {
	t.Helper()
	got := rulesForResource(rules, "secrets")
	if len(got) != 1 {
		t.Fatalf("expected exactly one Secret rule, got %#v", got)
	}
	want := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{name}, Verbs: []string{"get"}}
	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("Secret rule=%#v, want %#v", got[0], want)
	}
}

func assertRule(t *testing.T, rules []rbacv1.PolicyRule, want rbacv1.PolicyRule) {
	t.Helper()
	for _, got := range rules {
		if reflect.DeepEqual(got, want) {
			return
		}
	}
	t.Fatalf("missing exact RBAC rule %#v in %#v", want, rules)
}

func rulesForResource(rules []rbacv1.PolicyRule, resource string) []rbacv1.PolicyRule {
	got := make([]rbacv1.PolicyRule, 0, 1)
	for _, rule := range rules {
		for _, candidate := range rule.Resources {
			if candidate == "*" || candidate == resource {
				got = append(got, rule)
			}
		}
	}
	return got
}

func assertBinding(t *testing.T, got manifest, roleKind, roleName, subjectName, subjectNamespace string) {
	t.Helper()
	if got.RoleRef.APIGroup != rbacv1.GroupName || got.RoleRef.Kind != roleKind || got.RoleRef.Name != roleName {
		t.Fatalf("unexpected roleRef: %#v", got.RoleRef)
	}
	want := []rbacv1.Subject{{Kind: "ServiceAccount", Name: subjectName, Namespace: subjectNamespace}}
	if !reflect.DeepEqual(got.Subjects, want) {
		t.Fatalf("subjects=%#v, want %#v", got.Subjects, want)
	}
}

func findManifest(t *testing.T, docs []manifest, kind, name string) manifest {
	t.Helper()
	for _, doc := range docs {
		if doc.Kind == kind && doc.Metadata.Name == name {
			return doc
		}
	}
	t.Fatalf("missing %s %s", kind, name)
	return manifest{}
}

func decodeManifests(t *testing.T, path ...string) []manifest {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate RBAC contract test")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
	file, err := os.Open(filepath.Join(append([]string{repoRoot}, path...)...))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close() //nolint:errcheck

	decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)
	docs := make([]manifest, 0, 8)
	for {
		doc := manifest{}
		if err := decoder.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				return docs
			}
			t.Fatal(err)
		}
		if doc.Kind != "" {
			docs = append(docs, doc)
		}
	}
}
