//go:build envtest

// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpointcheck

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/credentials"
	"github.com/torrescd/provider-runpod/internal/inference"
)

const (
	// Keep both the Kubernetes release and the release index immutable. The
	// controller-runtime downloader verifies the archive SHA-512 from this
	// commit-pinned index before extracting any executable.
	envtestKubernetesVersion = "1.35.0"
	envtestReleaseIndexURL   = "https://raw.githubusercontent.com/kubernetes-sigs/controller-tools/d8c5ce13729ac2097e056934e0d686cc846693bd/envtest-releases.yaml"
)

type successfulEnvtestVerifier struct{}

func (successfulEnvtestVerifier) CheckHealth(context.Context) error { return nil }

func (successfulEnvtestVerifier) Verify(_ context.Context, expectedModelID string) (inference.Result, error) {
	if expectedModelID != "org/model" {
		return inference.Result{}, errors.New("unexpected model in mocked verifier")
	}
	return inference.Result{Healthy: true, ModelVerified: true, ToolCallVerified: true}, nil
}

// TestEnvtestAdmissionAndEndpointCheckController exercises the generated CRDs
// against a real kube-apiserver and drives the controller through its watch.
// The inference verifier is replaced before the manager starts, so this test
// cannot make a request to RunPod.
func TestEnvtestAdmissionAndEndpointCheckController(t *testing.T) {
	scheme := newEnvtestScheme(t)
	testEnvironment := &envtest.Environment{
		Scheme:                       scheme,
		CRDDirectoryPaths:            []string{envtestCRDDirectory(t)},
		ErrorIfCRDPathMissing:        true,
		DownloadBinaryAssets:         true,
		DownloadBinaryAssetsVersion:  envtestKubernetesVersion,
		DownloadBinaryAssetsIndexURL: envtestReleaseIndexURL,
		ControlPlaneStartTimeout:     60 * time.Second,
		ControlPlaneStopTimeout:      60 * time.Second,
	}
	cfg, err := testEnvironment.Start()
	if err != nil {
		t.Fatalf("start envtest control plane: %v", err)
	}

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
		Cache: cache.Options{DefaultNamespaces: map[string]cache.Config{
			"team-a": {},
		}},
		Metrics:                server.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
	})
	if err != nil {
		_ = testEnvironment.Stop()
		t.Fatalf("create controller manager: %v", err)
	}

	originalVerifier := newVerifier
	newVerifier = func(endpointID string, token []byte, _ time.Duration) (verifier, error) {
		if endpointID != "endpoint_1" || string(token) != "endpoint-only" {
			return nil, errors.New("unexpected endpoint credentials in mocked verifier")
		}
		return successfulEnvtestVerifier{}, nil
	}
	if err := Setup(mgr); err != nil {
		newVerifier = originalVerifier
		_ = testEnvironment.Stop()
		t.Fatalf("setup EndpointCheck controller: %v", err)
	}

	managerContext, cancelManager := context.WithCancel(context.Background())
	managerStopped := make(chan error, 1)
	go func() { managerStopped <- mgr.Start(managerContext) }()
	t.Cleanup(func() {
		cancelManager()
		if err := <-managerStopped; err != nil {
			t.Errorf("stop controller manager: %v", err)
		}
		newVerifier = originalVerifier
		if err := testEnvironment.Stop(); err != nil {
			t.Errorf("stop envtest control plane: %v", err)
		}
	})
	if !mgr.GetCache().WaitForCacheSync(managerContext) {
		t.Fatal("controller manager cache did not synchronize")
	}

	kube, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create uncached envtest client: %v", err)
	}
	for _, namespace := range []string{"team-a", "team-b"} {
		if err := kube.Create(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}); err != nil {
			t.Fatalf("create namespace %q: %v", namespace, err)
		}
		template := validEnvtestTemplate(namespace)
		if err := kube.Create(context.Background(), template); err != nil {
			t.Fatalf("create managed Template in %s: %v", namespace, err)
		}
		meta.SetExternalName(template, "template_1")
		if err := kube.Update(context.Background(), template); err != nil {
			t.Fatalf("publish managed Template external ID in %s: %v", namespace, err)
		}
		template.Status.ObservedGeneration = template.Generation
		template.Status.AtProvider.ID = "template_1"
		template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
		if err := kube.Status().Update(context.Background(), template); err != nil {
			t.Fatalf("publish managed Template status in %s: %v", namespace, err)
		}
	}

	t.Run("generated CRDs enforce security and lifetime constraints", func(t *testing.T) {
		missingVolume := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "serverless.runpod.crossplane.io/v1alpha1",
			"kind":       "Template",
			"metadata": map[string]any{
				"name": "missing-volume", "namespace": "team-a",
			},
			"spec": map[string]any{
				"forProvider": map[string]any{
					"name":      "missing-volume",
					"imageName": "ghcr.io/example/model@sha256:" + strings.Repeat("a", 64),
					"ports":     []any{"8000/http"},
				},
			},
		}}
		if err := kube.Create(context.Background(), missingVolume); !apierrors.IsInvalid(err) {
			t.Fatalf("Template without explicit volumeInGb should be rejected, got %v", err)
		}

		invalidTemplate := &serverlessv1alpha1.Template{
			ObjectMeta: metav1.ObjectMeta{Name: "mutable-image", Namespace: "team-a"},
			Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
				Name: "mutable-image", ImageName: "ghcr.io/example/model:latest", Ports: []string{"8000/http"},
			}},
		}
		if err := kube.Create(context.Background(), invalidTemplate); !apierrors.IsInvalid(err) {
			t.Fatalf("mutable image should be rejected by API admission, got %v", err)
		}
		invalidTemplate.Spec.ForProvider.ImageName = "ghcr.io/example/model@sha256:" + strings.Repeat("a", 64)
		invalidTemplate.Spec.ForProvider.VolumeInGB = 1
		if err := kube.Create(context.Background(), invalidTemplate); !apierrors.IsInvalid(err) {
			t.Fatalf("persistent Template volume should be rejected, got %v", err)
		}

		invalidProviderConfig := &apisv1alpha1.ProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "invalid-source", Namespace: "team-a"},
			Spec: apisv1alpha1.ProviderConfigSpec{Credentials: apisv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSource("Environment"),
			}},
		}
		if err := kube.Create(context.Background(), invalidProviderConfig); !apierrors.IsInvalid(err) {
			t.Fatalf("non-Secret ProviderConfig source should be rejected, got %v", err)
		}
		missingSecretRef := &apisv1alpha1.ProviderConfig{
			ObjectMeta: metav1.ObjectMeta{Name: "missing-secret-ref", Namespace: "team-a"},
			Spec: apisv1alpha1.ProviderConfigSpec{Credentials: apisv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
			}},
		}
		if err := kube.Create(context.Background(), missingSecretRef); !apierrors.IsInvalid(err) {
			t.Fatalf("ProviderConfig without secretRef should be rejected, got %v", err)
		}

		invalidEndpoint := validEnvtestEndpoint("invalid-capacity", "team-a")
		invalidEndpoint.Spec.ForProvider.WorkersMax = 2
		if err := kube.Create(context.Background(), invalidEndpoint); !apierrors.IsInvalid(err) {
			t.Fatalf("workersMax above one should be rejected, got %v", err)
		}
		scaleToZeroEndpoint := validEnvtestEndpoint("scale-to-zero", "team-a")
		scaleToZeroEndpoint.Spec.ForProvider.WorkersMin = 0
		if err := kube.Create(context.Background(), scaleToZeroEndpoint); !apierrors.IsInvalid(err) {
			t.Fatalf("workersMin below one should be rejected, got %v", err)
		}
		zeroMaxEndpoint := validEnvtestEndpoint("zero-max", "team-a")
		zeroMaxEndpoint.Spec.ForProvider.WorkersMax = 0
		if err := kube.Create(context.Background(), zeroMaxEndpoint); !apierrors.IsInvalid(err) {
			t.Fatalf("workersMax below one should be rejected, got %v", err)
		}

		observeOnlyEndpoint := validEnvtestEndpoint("observe-only", "team-a")
		observeOnlyEndpoint.Spec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionObserve}
		if err := kube.Create(context.Background(), observeOnlyEndpoint); !apierrors.IsInvalid(err) {
			t.Fatalf("Endpoint without full delete lifecycle should be rejected, got %v", err)
		}

		missingTemplate := validEnvtestEndpoint("missing-template", "team-a")
		missingTemplate.Spec.ForProvider.TemplateIDRef = nil
		if err := kube.Create(context.Background(), missingTemplate); !apierrors.IsInvalid(err) {
			t.Fatalf("Endpoint without exactly one template selector should be rejected, got %v", err)
		}

		missingEndpointRef := validEnvtestCheck("missing-endpoint-ref", "team-a")
		missingEndpointRef.Spec.ForProvider.EndpointIDRef = nil
		if err := kube.Create(context.Background(), missingEndpointRef); !apierrors.IsInvalid(err) {
			t.Fatalf("EndpointCheck without its managed Endpoint reference should be rejected, got %v", err)
		}

		endpointA := validEnvtestEndpoint("bounded", "team-a")
		if err := kube.Create(context.Background(), endpointA); err != nil {
			t.Fatalf("create valid Endpoint: %v", err)
		}
		if endpointA.Spec.ForProvider.DeleteReferencedTemplateOnExpiry == nil || !*endpointA.Spec.ForProvider.DeleteReferencedTemplateOnExpiry {
			t.Fatal("deleteReferencedTemplateOnExpiry was not defaulted to true")
		}
		endpointA.Spec.ForProvider.MaxLifetimeSeconds = 7200
		if err := kube.Update(context.Background(), endpointA); !apierrors.IsInvalid(err) {
			t.Fatalf("maxLifetimeSeconds mutation should be rejected, got %v", err)
		}

		endpointB := validEnvtestEndpoint("bounded", "team-b")
		if err := kube.Create(context.Background(), endpointB); err != nil {
			t.Fatalf("same Endpoint name should be accepted in a different namespace: %v", err)
		}
		persistedEndpoint := &serverlessv1alpha1.Endpoint{}
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(endpointB), persistedEndpoint); err != nil {
			t.Fatalf("get initial Endpoint status: %v", err)
		}
		if persistedEndpoint.Status.AtProvider.Version != nil || persistedEndpoint.Status.AtProvider.FlashBootEvidenceVersion != nil {
			t.Fatalf("pre-observation version evidence must remain absent: %#v", persistedEndpoint.Status.AtProvider)
		}
		zero := int32(0)
		persistedEndpoint.Status.AtProvider.ID = "endpoint_zero"
		persistedEndpoint.Status.AtProvider.Version = &zero
		persistedEndpoint.Status.AtProvider.FlashBootDisabled = true
		persistedEndpoint.Status.AtProvider.FlashBootEvidenceVersion = &zero
		if err := kube.Status().Update(context.Background(), persistedEndpoint); err != nil {
			t.Fatalf("persist valid RunPod version zero in Endpoint status: %v", err)
		}
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(endpointB), persistedEndpoint); err != nil {
			t.Fatalf("read Endpoint version-zero status: %v", err)
		}
		if persistedEndpoint.Status.AtProvider.Version == nil || *persistedEndpoint.Status.AtProvider.Version != 0 ||
			persistedEndpoint.Status.AtProvider.FlashBootEvidenceVersion == nil || *persistedEndpoint.Status.AtProvider.FlashBootEvidenceVersion != 0 {
			t.Fatalf("valid explicit version zero was pruned: %#v", persistedEndpoint.Status.AtProvider)
		}
		zeroCheck := validEnvtestCheck("version-zero", "team-b")
		if err := kube.Create(context.Background(), zeroCheck); err != nil {
			t.Fatalf("create EndpointCheck for version-zero status: %v", err)
		}
		zeroCheck.Status.ObservedGeneration = zeroCheck.Generation
		zeroCheck.Status.AtProvider.EndpointVersion = &zero
		if err := kube.Status().Update(context.Background(), zeroCheck); err != nil {
			t.Fatalf("persist valid RunPod version zero in EndpointCheck status: %v", err)
		}
		if err := kube.Get(context.Background(), client.ObjectKeyFromObject(zeroCheck), zeroCheck); err != nil {
			t.Fatalf("read EndpointCheck version-zero status: %v", err)
		}
		if zeroCheck.Status.AtProvider.EndpointVersion == nil || *zeroCheck.Status.AtProvider.EndpointVersion != 0 {
			t.Fatalf("valid explicit EndpointCheck version zero was pruned: %#v", zeroCheck.Status.AtProvider)
		}
		listed := &serverlessv1alpha1.EndpointList{}
		if err := kube.List(context.Background(), listed, client.InNamespace("team-a")); err != nil {
			t.Fatalf("list team-a Endpoints: %v", err)
		}
		if len(listed.Items) != 1 || listed.Items[0].Namespace != "team-a" {
			t.Fatalf("namespaced Endpoint isolation failed: %#v", listed.Items)
		}
	})

	t.Run("namespace-scoped controller admits only inference-purpose credentials", func(t *testing.T) {
		for _, namespace := range []string{"team-a", "team-b"} {
			endpoint := &serverlessv1alpha1.Endpoint{}
			key := types.NamespacedName{Name: "bounded", Namespace: namespace}
			if err := kube.Get(context.Background(), key, endpoint); err != nil {
				t.Fatalf("get bounded Endpoint in %s: %v", namespace, err)
			}
			meta.SetExternalName(endpoint, "endpoint_1")
			if err := kube.Update(context.Background(), endpoint); err != nil {
				t.Fatalf("publish bounded Endpoint external ID in %s: %v", namespace, err)
			}
			endpoint.Status.ObservedGeneration = endpoint.Generation
			endpoint.Status.AtProvider.ID = "endpoint_1"
			template := &serverlessv1alpha1.Template{}
			if err := kube.Get(context.Background(), types.NamespacedName{Name: "template", Namespace: namespace}, template); err != nil {
				t.Fatalf("get managed Template in %s: %v", namespace, err)
			}
			endpoint.Status.AtProvider.TemplateID = "template_1"
			endpoint.Status.AtProvider.TemplateResourceUID = string(template.UID)
			endpoint.Status.AtProvider.TemplateResourceGeneration = template.Generation
			endpoint.Status.AtProvider.TemplateImageDigest = template.Spec.ForProvider.ImageName
			endpoint.Status.AtProvider.Version = envtestInt32(1)
			endpoint.Status.AtProvider.FlashBootDisabled = true
			endpoint.Status.AtProvider.FlashBootEvidenceVersion = envtestInt32(1)
			endpoint.Status.AtProvider.FlashBootLastEnforcedAt = metav1.Now()
			endpoint.Status.AtProvider.WorkerSecurityValidated = true
			endpoint.Status.AtProvider.WorkerSecurityProofVersion = envtestInt32(1)
			endpoint.Status.AtProvider.WorkerSecurityObservedAt = metav1.Now()
			endpoint.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
			if err := kube.Status().Update(context.Background(), endpoint); err != nil {
				t.Fatalf("publish bounded Endpoint status in %s: %v", namespace, err)
			}
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name: "inference", Namespace: namespace,
					Labels: map[string]string{credentials.PurposeLabel: credentials.PurposeInference},
				},
				Data: map[string][]byte{"token": []byte("endpoint-only")},
			}
			if err := kube.Create(context.Background(), secret); err != nil {
				t.Fatalf("create inference Secret in %s: %v", namespace, err)
			}
		}

		teamACheck := validEnvtestCheck("route", "team-a")
		if err := kube.Create(context.Background(), teamACheck); err != nil {
			t.Fatalf("create team-a EndpointCheck: %v", err)
		}
		teamACheck = awaitEndpointCheckCondition(t, kube, client.ObjectKeyFromObject(teamACheck), corev1.ConditionTrue)

		wrongPurpose := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: "management", Namespace: "team-a",
				Labels: map[string]string{credentials.PurposeLabel: credentials.PurposeManagement},
			},
			Data: map[string][]byte{"token": []byte("endpoint-only")},
		}
		if err := kube.Create(context.Background(), wrongPurpose); err != nil {
			t.Fatalf("create wrong-purpose Secret: %v", err)
		}

		blocked := validEnvtestCheck("blocked", "team-a")
		blocked.Spec.ForProvider.InferenceCredentialsSecretRef.Name = "management"
		if err := kube.Create(context.Background(), blocked); err != nil {
			t.Fatalf("create blocked EndpointCheck: %v", err)
		}
		blocked = awaitEndpointCheckCondition(t, kube, client.ObjectKeyFromObject(blocked), corev1.ConditionFalse)
		if message := blocked.Status.GetCondition(xpv2.TypeReady).Message; !strings.Contains(message, credentials.PurposeLabel) {
			t.Fatalf("unexpected credential-separation failure: %q", message)
		}
		if !containsString(teamACheck.Finalizers, RouterDrainFinalizer) {
			t.Fatal("controller did not add the router drain finalizer")
		}
	})
}

func newEnvtestScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	for _, add := range []func(*k8sruntime.Scheme) error{
		clientgoscheme.AddToScheme,
		apisv1alpha1.SchemeBuilder.AddToScheme,
		serverlessv1alpha1.SchemeBuilder.AddToScheme,
		verificationv1alpha1.SchemeBuilder.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add API to envtest scheme: %v", err)
		}
	}
	return scheme
}

func envtestCRDDirectory(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate envtest source file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "..", "..", "..", "package", "crds"))
}

func validEnvtestEndpoint(name, namespace string) *serverlessv1alpha1.Endpoint {
	return &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{
			MaxLifetimeSeconds:           3600,
			Name:                         name,
			TemplateIDRef:                &xpv2.Reference{Name: "template"},
			GPUTypeIDs:                   []string{"NVIDIA GeForce RTX 4090"},
			DataCenterIDs:                []string{"EU-RO-1"},
			WorkersMin:                   1,
			WorkersMax:                   1,
			MaxWorkerCostMilliUSDPerHour: 2000,
			IdleTimeout:                  5,
			ScalerType:                   "QUEUE_DELAY",
			ScalerValue:                  1,
			ExecutionTimeoutMS:           1000,
		}},
	}
}

func validEnvtestTemplate(namespace string) *serverlessv1alpha1.Template {
	return &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: namespace},
		Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
			MaxLifetimeSeconds: 3600,
			Name:               "template",
			ImageName:          "ghcr.io/example/model@sha256:" + strings.Repeat("a", 64),
			Ports:              []string{"8000/http"},
			VolumeInGB:         0,
		}},
	}
}

func validEnvtestCheck(name, namespace string) *verificationv1alpha1.EndpointCheck {
	return &verificationv1alpha1.EndpointCheck{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{
			MaxLifetimeSeconds:          3600,
			EndpointIDRef:               &xpv2.Reference{Name: "bounded"},
			ExpectedModelID:             "org/model",
			VerificationIntervalSeconds: 3600,
			InferenceCredentialsSecretRef: xpv2.LocalSecretKeySelector{
				LocalSecretReference: xpv2.LocalSecretReference{Name: "inference"}, Key: "token",
			},
			TimeoutSeconds: 180,
		}},
	}
}

func awaitEndpointCheckCondition(t *testing.T, kube client.Client, key types.NamespacedName, want corev1.ConditionStatus) *verificationv1alpha1.EndpointCheck {
	t.Helper()
	got := &verificationv1alpha1.EndpointCheck{}
	err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 20*time.Second, true, func(ctx context.Context) (bool, error) {
		if err := kube.Get(ctx, key, got); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		return got.Status.GetCondition(xpv2.TypeReady).Status == want, nil
	})
	if err != nil {
		t.Fatalf("wait for EndpointCheck %s Ready=%s: %v (status: %#v)", key, want, err, got.Status)
	}
	return got
}

func envtestInt32(value int32) *int32 { return &value }
