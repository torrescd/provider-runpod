// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpoint

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	resourcefake "github.com/crossplane/crossplane-runtime/v2/pkg/resource/fake"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
)

type fakeEndpointService struct {
	created   clientrunpod.EndpointInput
	creates   int
	createErr error
	recovered *clientrunpod.Endpoint
	findErr   error
	findName  string
	deletes   int
	updates   int
	got       *clientrunpod.Endpoint
	gets      int
}

func (f *fakeEndpointService) GetEndpoint(context.Context, string) (*clientrunpod.Endpoint, error) {
	f.gets++
	if f.got != nil {
		return f.got, nil
	}
	return nil, clientrunpod.ErrNotFound
}
func (f *fakeEndpointService) FindEndpointByName(_ context.Context, name string) (*clientrunpod.Endpoint, error) {
	f.findName = name
	if f.findErr != nil {
		return nil, f.findErr
	}
	if f.recovered == nil {
		return nil, clientrunpod.ErrNotFound
	}
	return f.recovered, nil
}
func (f *fakeEndpointService) CreateEndpoint(_ context.Context, in clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error) {
	f.creates++
	f.created = in
	if f.createErr != nil {
		return nil, f.createErr
	}
	return endpointFromInput("endpoint_1", in), nil
}

func TestAmbiguousCreatePersistsRecoveryAndNeverPostsTwice(t *testing.T) {
	cr := endpointCR()
	svc := &fakeEndpointService{createErr: clientrunpod.ErrCreateAmbiguous}
	e := &external{service: svc, template: boundedTemplateBinding()}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	pendingName := cr.Annotations[ambiguousCreateNameAnnotation]
	if pendingName == "" || svc.created.Name == "" || svc.creates != 1 || meta.GetExternalName(cr) != "" {
		t.Fatalf("pending=%q created=%+v POSTs=%d external=%q", pendingName, svc.created, svc.creates, meta.GetExternalName(cr))
	}
	firstName := svc.created.Name
	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("not-yet-visible ambiguous endpoint create did not fail closed")
	}
	if _, err := e.Create(context.Background(), cr); err != nil || svc.created.Name != firstName || svc.creates != 1 {
		t.Fatalf("recovery attempted a second POST: err=%v created=%+v POSTs=%d", err, svc.created, svc.creates)
	}
	svc.recovered = endpointFromInput("endpoint_eventual", svc.created)
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || meta.GetExternalName(cr) != "endpoint_eventual" || cr.Annotations[ambiguousCreateNameAnnotation] != "" {
		t.Fatalf("eventual recovery observation=%+v external=%q marker=%q err=%v", observed, meta.GetExternalName(cr), cr.Annotations[ambiguousCreateNameAnnotation], err)
	}
}

func TestRuntimeIncompleteCreateRecoversByUIDNameWithoutSecondPost(t *testing.T) {
	cr := endpointCR()
	meta.SetExternalCreatePending(cr, time.Now())
	svc := &fakeEndpointService{}
	e := &external{service: svc, template: boundedTemplateBinding()}
	if _, err := e.Observe(context.Background(), cr); err == nil || svc.creates != 0 {
		t.Fatalf("incomplete create not-found did not fail closed: err=%v POSTs=%d", err, svc.creates)
	}
	desired := endpointInput(cr, boundedTemplateBinding().id)
	desired.Name = effectiveName(cr)
	svc.recovered = endpointFromInput("endpoint_recovered", desired)
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || !observed.ResourceUpToDate || svc.creates != 0 || meta.GetExternalName(cr) != "endpoint_recovered" {
		t.Fatalf("crash recovery observation=%+v external=%q POSTs=%d err=%v", observed, meta.GetExternalName(cr), svc.creates, err)
	}
}

func TestUnverifiedEndpointExternalNameCannotAdoptByKubernetesName(t *testing.T) {
	cr := endpointCR()
	cr.Name = "endpoint_existing"
	meta.SetExternalName(cr, cr.Name)
	svc := &fakeEndpointService{got: endpointFromInput(cr.Name, endpointInput(cr, "tpl_1"))}
	if _, err := (&external{service: svc, template: boundedTemplateBinding()}).Observe(context.Background(), cr); err == nil || svc.gets != 0 {
		t.Fatalf("unverified external-name was queried/adopted: err=%v GETs=%d", err, svc.gets)
	}
}

func TestForgedBoundEndpointIDCannotUpdateOrDeleteArbitraryResource(t *testing.T) {
	newCR := func() *serverlessv1alpha1.Endpoint {
		cr := endpointCR()
		meta.SetExternalName(cr, "endpoint_victim")
		cr.Annotations[externalIDBoundAnnotation] = "true"
		return cr
	}
	wrong := endpointFromInput("endpoint_victim", endpointInput(newCR(), "tpl_1"))
	wrong.Name = "someone-elses-endpoint"
	svc := &fakeEndpointService{got: wrong}
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	reader := fake.NewClientBuilder().WithScheme(scheme).Build()
	e := &external{service: svc, reader: reader, template: boundedTemplateBinding()}

	cr := newCR()
	if _, err := e.Observe(context.Background(), cr); err == nil || svc.gets != 1 {
		t.Fatal("forged bound marker was accepted without an exact-name GET rejection")
	}
	if _, err := e.Update(context.Background(), cr); err == nil || svc.updates != 0 {
		t.Fatalf("forged bound marker reached Endpoint PATCH: err=%v updates=%d", err, svc.updates)
	}

	deleting := newCR()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if _, err := e.Observe(context.Background(), deleting); err == nil || svc.gets != 2 {
		t.Fatal("deleting forged bound marker was accepted by Observe")
	}
	if _, err := e.Delete(context.Background(), deleting); err == nil || svc.deletes != 0 {
		t.Fatalf("forged bound marker reached Endpoint DELETE: err=%v deletes=%d", err, svc.deletes)
	}

	owned := newCR()
	bindExternalID(owned, "endpoint_owned")
	svc.got = endpointFromInput("endpoint_owned", endpointInput(owned, "tpl_1"))
	svc.got.Name = "externally-renamed"
	if _, err := e.Delete(context.Background(), owned); err != nil || svc.deletes != 1 {
		t.Fatalf("durably bound Endpoint could not be cleaned up after external name drift: err=%v deletes=%d", err, svc.deletes)
	}
}

func TestBoundEndpointNotFoundNeverCreatesReplacementButDeletionCompletes(t *testing.T) {
	cr := endpointCR()
	bindExternalID(cr, "endpoint_missing")
	svc := &fakeEndpointService{}
	e := &external{service: svc, template: boundedTemplateBinding()}
	if _, err := e.Observe(context.Background(), cr); err == nil || svc.creates != 0 {
		t.Fatalf("bound 404 permitted replacement create: err=%v creates=%d", err, svc.creates)
	}
	deleting := cr.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	observed, err := e.Observe(context.Background(), deleting)
	if err != nil || observed.ResourceExists || svc.creates != 0 {
		t.Fatalf("deleting bound 404 did not converge: observation=%+v err=%v creates=%d", observed, err, svc.creates)
	}
}

func TestCreateCriticalAnnotationBoundaryRepopulatesEndpointStatus(t *testing.T) {
	cr := endpointCR()
	svc := &fakeEndpointService{}
	e := &external{service: svc, template: boundedTemplateBinding()}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	id := meta.GetExternalName(cr)
	cr.Status = serverlessv1alpha1.EndpointStatus{}
	desired := endpointInput(cr, boundedTemplateBinding().id)
	svc.got = endpointFromInput(id, desired)
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || cr.Status.AtProvider.ID != id {
		t.Fatalf("critical-annotation recovery observation=%+v status=%+v err=%v", observed, cr.Status.AtProvider, err)
	}
}

func TestManagedReconcilerPersistsAmbiguousEndpointRecoveryBinding(t *testing.T) {
	cr := endpointCR()
	cr.Spec.ManagedResourceSpec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionAll}
	markAmbiguousCreate(cr, effectiveName(cr))
	desired := endpointInput(cr, boundedTemplateBinding().id)
	desired.Name = effectiveName(cr)
	svc := &fakeEndpointService{recovered: endpointFromInput("endpoint_recovered", desired)}
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr).Build()
	mgr := &resourcefake.Manager{Client: kube, Scheme: scheme}
	ext := &external{service: svc, reader: kube, template: boundedTemplateBinding()}
	r := managed.NewReconciler(mgr, resource.ManagedKind(serverlessv1alpha1.EndpointGroupVersionKind),
		managed.WithInitializers(),
		managed.WithDeterministicExternalName(true),
		managed.WithManagementPolicies(),
		managed.WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(context.Context, resource.Object) error { return nil }}),
		managed.WithTypedExternalConnector(managed.TypedExternalConnectorFn[*serverlessv1alpha1.Endpoint](func(context.Context, *serverlessv1alpha1.Endpoint) (managed.TypedExternalClient[*serverlessv1alpha1.Endpoint], error) {
			return ext, nil
		})),
	)
	key := types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatal(err)
	}
	persisted := &serverlessv1alpha1.Endpoint{}
	if err := kube.Get(context.Background(), key, persisted); err != nil {
		t.Fatal(err)
	}
	if meta.GetExternalName(persisted) != "endpoint_recovered" || persisted.Annotations[externalIDBoundAnnotation] != "true" || persisted.Annotations[ambiguousCreateNameAnnotation] != "" {
		t.Fatalf("managed reconcile did not persist recovered metadata: %#v", persisted.Annotations)
	}
}

func TestManagedReconcilerRetainsAmbiguousEndpointUntilLateVisibility(t *testing.T) {
	deletion := metav1.NewTime(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	cr := endpointCR()
	cr.Spec.ManagedResourceSpec.ManagementPolicies = xpv2.ManagementPolicies{xpv2.ManagementActionAll}
	cr.DeletionTimestamp = &deletion
	cr.Finalizers = []string{"finalizer.managedresource.crossplane.io"}
	meta.SetExternalCreatePending(cr, deletion.Time.Add(-time.Minute))
	markAmbiguousCreate(cr, effectiveName(cr))
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr).Build()
	svc := &fakeEndpointService{}
	ext := &external{service: svc, kube: kube, reader: kube}
	removed := 0
	r := managed.NewReconciler(&resourcefake.Manager{Client: kube, Scheme: scheme}, resource.ManagedKind(serverlessv1alpha1.EndpointGroupVersionKind),
		managed.WithInitializers(), managed.WithDeterministicExternalName(true), managed.WithManagementPolicies(),
		managed.WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(context.Context, resource.Object) error { return nil },
			RemoveFinalizerFn: func(context.Context, resource.Object) error { removed++; return nil },
		}),
		managed.WithTypedExternalConnector(managed.TypedExternalConnectorFn[*serverlessv1alpha1.Endpoint](func(context.Context, *serverlessv1alpha1.Endpoint) (managed.TypedExternalClient[*serverlessv1alpha1.Endpoint], error) {
			return ext, nil
		})),
	)
	request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}
	_, _ = r.Reconcile(context.Background(), request)
	if removed != 0 || svc.deletes != 0 {
		t.Fatalf("one absent list read released ambiguous Endpoint: finalizer removals=%d deletes=%d", removed, svc.deletes)
	}

	desired := endpointInput(cr, "tpl_1")
	desired.Name = effectiveName(cr)
	svc.recovered = endpointFromInput("endpoint_late", desired)
	svc.got = svc.recovered
	for i := 0; i < 4 && svc.deletes == 0; i++ {
		_, _ = r.Reconcile(context.Background(), request)
	}
	if svc.deletes != 1 {
		t.Fatalf("late-visible ambiguous Endpoint was not recovered and deleted: deletes=%d removals=%d", svc.deletes, removed)
	}
}

func TestDeletingAmbiguousEndpointRecoversIdentityBeforeDelete(t *testing.T) {
	cr := endpointCR()
	now := metav1.Now()
	cr.DeletionTimestamp = &now
	markAmbiguousCreate(cr, effectiveName(cr))
	desired := endpointInput(cr, "tpl_1")
	desired.Name = effectiveName(cr)
	svc := &fakeEndpointService{recovered: endpointFromInput("endpoint_cleanup", desired)}
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	e := &external{service: svc, reader: fake.NewClientBuilder().WithScheme(scheme).Build()}
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || meta.GetExternalName(cr) != "endpoint_cleanup" {
		t.Fatalf("cleanup recovery observation=%+v external=%q err=%v", observed, meta.GetExternalName(cr), err)
	}
	svc.got = svc.recovered
	if _, err := e.Delete(context.Background(), cr); err != nil || svc.deletes != 1 {
		t.Fatalf("cleanup delete err=%v calls=%d", err, svc.deletes)
	}

	cr2 := endpointCR()
	cr2.DeletionTimestamp = &now
	markAmbiguousCreate(cr2, effectiveName(cr2))
	ambiguous := &fakeEndpointService{findErr: clientrunpod.ErrAmbiguous}
	e.service = ambiguous
	if _, err := e.Observe(context.Background(), cr2); err == nil || meta.GetExternalName(cr2) != "" || ambiguous.deletes != 0 {
		t.Fatal("ambiguous cleanup identity was adopted or deleted")
	}

	absent := endpointCR()
	absent.Name = "absence-confirmation"
	absent.UID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	absent.DeletionTimestamp = &now
	meta.SetExternalCreatePending(absent, time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	markAmbiguousCreate(absent, effectiveName(absent))
	absenceService := &fakeEndpointService{}
	e = &external{service: absenceService, reader: fake.NewClientBuilder().WithScheme(scheme).Build()}
	if observed, err = e.Observe(context.Background(), absent); err == nil || observed.ResourceExists {
		t.Fatalf("unacknowledged ambiguous absence completed deletion: observation=%+v err=%v", observed, err)
	}
	absent.Annotations[ambiguousAbsenceConfirmedAnnotation] = "wrong-pending-token"
	if observed, err = e.Observe(context.Background(), absent); err == nil || observed.ResourceExists {
		t.Fatalf("mismatched absence acknowledgement completed deletion: observation=%+v err=%v", observed, err)
	}

	desired = endpointInput(absent, "tpl_1")
	desired.Name = effectiveName(absent)
	absenceService.recovered = endpointFromInput("endpoint_late", desired)
	observed, err = e.Observe(context.Background(), absent)
	if err != nil || !observed.ResourceExists || meta.GetExternalName(absent) != "endpoint_late" {
		t.Fatalf("late-visible Endpoint was not recovered for cleanup: observation=%+v err=%v", observed, err)
	}

	confirmed := endpointCR()
	confirmed.Name = "confirmed-absent"
	confirmed.UID = "ffffffff-1111-2222-3333-444444444444"
	confirmed.DeletionTimestamp = &now
	meta.SetExternalCreatePending(confirmed, time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC))
	markAmbiguousCreate(confirmed, effectiveName(confirmed))
	pendingToken := confirmed.Annotations[externalCreatePendingAnnotation]
	confirmed.Annotations[ambiguousAbsenceConfirmedAnnotation] = pendingToken
	e.service = &fakeEndpointService{}
	observed, err = e.Observe(context.Background(), confirmed)
	if err != nil || observed.ResourceExists {
		t.Fatalf("exact operator acknowledgement did not finalize deleting absence: observation=%+v err=%v", observed, err)
	}

	live := confirmed.DeepCopy()
	live.DeletionTimestamp = nil
	if _, err := e.Observe(context.Background(), live); err == nil {
		t.Fatal("live acknowledged Endpoint resumed reconciliation")
	}
	if _, err := e.Create(context.Background(), live); err == nil || e.service.(*fakeEndpointService).creates != 0 {
		t.Fatalf("live acknowledgement permitted Endpoint recreation: err=%v", err)
	}
}
func (f *fakeEndpointService) UpdateEndpoint(_ context.Context, id string, in clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error) {
	f.updates++
	return endpointFromInput(id, in), nil
}
func (f *fakeEndpointService) DeleteEndpoint(context.Context, string) error { f.deletes++; return nil }

func TestCreateEnforcesCostBoundsAndUniqueRecoveryName(t *testing.T) {
	cr := endpointCR()
	svc := &fakeEndpointService{}
	e := &external{service: svc, template: boundedTemplateBinding()}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if svc.created.WorkersMin != 1 || svc.created.WorkersMax != 1 || svc.created.GPUCount != 1 || svc.created.ComputeType != "GPU" || svc.created.FlashBoot {
		t.Fatalf("unsafe endpoint request: %+v", svc.created)
	}
	if svc.created.Name == cr.Spec.ForProvider.Name {
		t.Fatal("endpoint recovery name was not UID-scoped")
	}
	if meta.GetExternalName(cr) != "endpoint_1" {
		t.Fatal("external ID not recorded")
	}
}

func TestCreateRequiresExactlyOneContinuouslyProvisionedWorker(t *testing.T) {
	for name, mutate := range map[string]func(*serverlessv1alpha1.Endpoint){
		"zero minimum": func(cr *serverlessv1alpha1.Endpoint) { cr.Spec.ForProvider.WorkersMin = 0 },
		"zero maximum": func(cr *serverlessv1alpha1.Endpoint) { cr.Spec.ForProvider.WorkersMax = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			cr := endpointCR()
			mutate(cr)
			svc := &fakeEndpointService{}
			if _, err := (&external{service: svc, template: boundedTemplateBinding()}).Create(context.Background(), cr); err == nil || svc.creates != 0 {
				t.Fatalf("unsafe worker bounds reached RunPod: err=%v creates=%d", err, svc.creates)
			}
		})
	}
}

func TestObserveAdvancesGenerationOnlyForConfirmedCurrentEndpoint(t *testing.T) {
	cr := endpointCR()
	cr.Generation = 1
	bindExternalID(cr, "endpoint_1")
	want := endpointInput(cr, "tpl_1")
	svc := &fakeEndpointService{got: endpointFromInput("endpoint_1", want)}
	e := &external{service: svc, template: boundedTemplateBinding()}
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceUpToDate || cr.Status.ObservedGeneration != cr.Generation {
		t.Fatalf("initial Observe did not publish current generation: observation=%+v status=%+v err=%v", observed, cr.Status, err)
	}
	svc.got.WorkersMax = ptrInt32(0)
	observed, err = e.Observe(context.Background(), cr)
	if err != nil || observed.ResourceUpToDate || cr.Status.ObservedGeneration != 0 {
		t.Fatalf("drift retained a consumable generation: observation=%+v status=%+v err=%v", observed, cr.Status, err)
	}
}

func TestFlashBootReadinessRequiresProviderWriteEvidenceBoundToVersion(t *testing.T) {
	cr := endpointCR()
	cr.Generation = 1
	bindExternalID(cr, "endpoint_1")
	want := endpointInput(cr, "tpl_1")
	got := endpointFromInput("endpoint_1", want)
	got.FlashBoot = nil // The official v1 GET schema omits this field.
	svc := &fakeEndpointService{got: got}
	e := &external{service: svc, template: boundedTemplateBinding()}

	observed, err := e.Observe(context.Background(), cr)
	if err != nil || observed.ResourceUpToDate || cr.Status.AtProvider.FlashBootDisabled {
		t.Fatalf("unproven adoption became ready: observation=%+v status=%+v err=%v", observed, cr.Status.AtProvider, err)
	}
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	// Simulate the documented GET omission after the successful bounded PATCH.
	svc.got.FlashBoot = nil
	observed, err = e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceUpToDate || !cr.Status.AtProvider.FlashBootDisabled {
		t.Fatalf("write evidence was not retained: observation=%+v status=%+v err=%v", observed, cr.Status.AtProvider, err)
	}
	// A template/environment rollout changes Version. The prior evidence is no
	// longer valid and routing must wait for another explicit PATCH.
	*svc.got.Version++
	observed, err = e.Observe(context.Background(), cr)
	if err != nil || observed.ResourceUpToDate || cr.Status.AtProvider.FlashBootDisabled {
		t.Fatalf("stale FlashBoot evidence survived a rollout: observation=%+v status=%+v err=%v", observed, cr.Status.AtProvider, err)
	}
}

func TestFlashBootEvidenceExpiresAndForcesBoundedReassertion(t *testing.T) {
	cr := endpointCR()
	cr.Generation = 1
	bindExternalID(cr, "endpoint_1")
	want := endpointInput(cr, "tpl_1")
	got := endpointFromInput("endpoint_1", want)
	got.FlashBoot = nil
	svc := &fakeEndpointService{got: got}
	e := &external{service: svc, template: boundedTemplateBinding()}
	cr.Status.AtProvider.ID = got.ID
	cr.Status.AtProvider.Version = got.Version
	cr.Status.AtProvider.FlashBootDisabled = true
	cr.Status.AtProvider.FlashBootEvidenceVersion = got.Version
	cr.Status.AtProvider.FlashBootLastEnforcedAt = metav1.NewTime(time.Now().Add(-serverlessv1alpha1.FlashBootEvidenceMaxAge - time.Second))

	observed, err := e.Observe(context.Background(), cr)
	if err != nil || observed.ResourceUpToDate || cr.Status.AtProvider.FlashBootDisabled {
		t.Fatalf("expired evidence remained ready: observation=%+v status=%+v err=%v", observed, cr.Status.AtProvider, err)
	}
	if _, err := e.Update(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if svc.updates != 1 || !cr.Status.AtProvider.FlashBootEvidenceCurrent(time.Now()) {
		t.Fatalf("bounded reassertion calls=%d status=%+v", svc.updates, cr.Status.AtProvider)
	}
}

func TestTemplateRevisionWaitsForRunPodRolloutVersion(t *testing.T) {
	cr := endpointCR()
	oldBinding := boundedTemplateBinding()
	newBinding := oldBinding
	newBinding.generation++
	newBinding.image = "registry/model@sha256:" + strings.Repeat("b", 64)
	cr.Status.AtProvider = serverlessv1alpha1.EndpointObservation{
		ID: "endpoint_1", Version: ptrInt32(7),
		TemplateResourceUID: oldBinding.uid, TemplateResourceGeneration: oldBinding.generation, TemplateImageDigest: oldBinding.image,
	}
	got := endpointFromInput("endpoint_1", endpointInput(cr, oldBinding.id))
	got.Version = ptrInt32(7)
	_, templateProven := setObservation(cr, got, false, newBinding, false)
	if templateProven || cr.Status.AtProvider.TemplateResourceGeneration != oldBinding.generation {
		t.Fatal("new Template revision was bound before RunPod reported a rollout version change")
	}
	got.Version = ptrInt32(8)
	_, templateProven = setObservation(cr, got, false, newBinding, false)
	if !templateProven || cr.Status.AtProvider.TemplateResourceGeneration != newBinding.generation || cr.Status.AtProvider.TemplateImageDigest != newBinding.image {
		t.Fatal("new Template revision was not bound after RunPod rollout version advanced")
	}
}

func TestWorkerSecurityProofClearsWhenWorkerDisappearsAndRequiresReobservation(t *testing.T) {
	cr := endpointCR()
	got := endpointFromInput("endpoint_1", endpointInput(cr, "tpl_1"))
	binding := boundedTemplateBinding()
	setObservation(cr, got, true, binding, true)
	if !cr.Status.AtProvider.WorkerSecurityEvidenceCurrent() {
		t.Fatal("validated active worker did not establish security proof")
	}

	// The next direct management observation reports no worker. Retaining
	// the prior worker's cost/SecureCloud evidence would let an unobserved future
	// placement receive traffic, so it must be withdrawn immediately.
	setObservation(cr, got, false, binding, false)
	if cr.Status.AtProvider.WorkerSecurityValidated || cr.Status.AtProvider.WorkerSecurityProofVersion != nil ||
		!cr.Status.AtProvider.WorkerSecurityObservedAt.IsZero() || cr.Status.AtProvider.WorkerSecurityEvidenceCurrent() {
		t.Fatalf("empty-worker observation retained historical proof: %+v", cr.Status.AtProvider)
	}

	// A later wake is not admitted until a new nonempty worker snapshot has
	// passed all endpointWorkersMatch checks and reaches setObservation as proven.
	setObservation(cr, got, false, binding, true)
	if !cr.Status.AtProvider.WorkerSecurityEvidenceCurrent() {
		t.Fatal("new bounded active-worker observation did not restore proof")
	}
}

func TestTemplateEventsEnqueueEveryReferencingEndpoint(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system"}}
	matching := endpointCR()
	matching.Name = "matching"
	other := endpointCR()
	other.Name = "other"
	other.Spec.ForProvider.TemplateIDRef.Name = "different"
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(matching, other).Build()
	requests := requestsForTemplate(context.Background(), kube, template)
	if len(requests) != 1 || requests[0].Name != matching.Name || requests[0].Namespace != matching.Namespace {
		t.Fatalf("Template event requests=%v", requests)
	}
}

func TestExpiredEndpointNeverCreatesOrUpdatesExternally(t *testing.T) {
	cr := endpointCR()
	cr.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Hour))
	svc := &fakeEndpointService{}
	e := &external{service: svc, template: boundedTemplateBinding()}
	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("expired Endpoint create was accepted")
	}
	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("expired Endpoint update was accepted")
	}
	if svc.creates != 0 || svc.updates != 0 {
		t.Fatalf("expired Endpoint made external calls: creates=%d updates=%d", svc.creates, svc.updates)
	}
}

func TestEndpointPersistentVolumeDriftFailsClosed(t *testing.T) {
	cr := endpointCR()
	want := endpointInput(cr, "tpl_1")
	got := endpointFromInput("endpoint_1", want)
	got.NetworkVolumeID = clientrunpod.StringValue("persistent-volume")
	if endpointMatchesInput(want, got) {
		t.Fatal("Endpoint with unmanaged networkVolumeId was accepted")
	}
	got.NetworkVolumeID = clientrunpod.StringValue("")
	got.NetworkVolumeIDs = []string{"persistent-volume"}
	if endpointMatchesInput(want, got) {
		t.Fatal("Endpoint with unmanaged networkVolumeIds was accepted")
	}
}

func TestEndpointUnsupportedSecurityFieldsFailClosed(t *testing.T) {
	cr := endpointCR()
	want := endpointInput(cr, "tpl_1")
	base := *endpointFromInput("endpoint_1", want)

	for name, mutate := range map[string]func(*clientrunpod.Endpoint){
		"remote environment":    func(got *clientrunpod.Endpoint) { got.Env = map[string]string{"SECRET": "unmanaged"} },
		"minimum CUDA override": func(got *clientrunpod.Endpoint) { got.MinCUDAVersion = clientrunpod.StringValue("13.0") },
		"allowed CUDA drift":    func(got *clientrunpod.Endpoint) { got.AllowedCUDAVersions = []string{"13.0"} },
	} {
		t.Run(name, func(t *testing.T) {
			got := base
			mutate(&got)
			if endpointMatchesInput(want, &got) {
				t.Fatalf("%s was accepted as bounded state", name)
			}
		})
	}
}

func TestActiveWorkerMustMatchBoundSecureTemplate(t *testing.T) {
	want := endpointInput(endpointCR(), "tpl_1")
	got := endpointFromInput("endpoint_1", want)
	got.Version = ptrInt32(0) // Official RunPod examples document version zero.
	secure := true
	locked := false
	interruptible := false
	// RunPod's official zero-volume Pod example reports false. Presence is the
	// proof; with volumeInGb=0 there is no local persistent volume to encrypt.
	volumeEncrypted := false
	count := int32(1)
	volume := int32(0)
	cost := 0.75
	workerID := "worker_1"
	got.Workers = []clientrunpod.EndpointWorker{{
		ID: clientrunpod.StringValue(workerID), EndpointID: clientrunpod.NullString(), TemplateID: clientrunpod.NullString(),
		AdjustedCostPerHr: clientrunpod.DecimalValue(cost), CostPerHr: clientrunpod.DecimalValue(cost), DesiredStatus: clientrunpod.StringValue("RUNNING"),
		Interruptible: &interruptible, PublicIP: clientrunpod.NullString(), PortMappings: json.RawMessage("{}"), SavingsPlans: json.RawMessage("[]"),
		SLSVersion: ptrInt32(0), Image: clientrunpod.StringValue(boundedTemplateBinding().image),
		ContainerDiskInGB: ptrInt32(50), ContainerRegistryAuthID: clientrunpod.NullString(),
		DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"}, Env: map[string]string{},
		VolumeInGB: &volume, VolumeMountPath: clientrunpod.NullString(), VolumeEncrypted: &volumeEncrypted, NetworkVolume: json.RawMessage("null"),
		GPU: &clientrunpod.EndpointWorkerGPU{Count: &count}, Machine: &clientrunpod.EndpointWorkerMachine{
			GPUTypeID: &want.GPUTypeIDs[0], DataCenterID: &want.DataCenterIDs[0], SecureCloud: &secure,
			CostPerHr: clientrunpod.DecimalValue(cost), CurrentPricePerGPU: clientrunpod.DecimalValue(cost),
		}, Locked: &locked,
	}}
	if !endpointWorkersMatch(want, got, boundedTemplateBinding(), 1000) {
		t.Fatal("official null association fields and version zero were not accepted for an otherwise bounded worker")
	}
	got.Workers[0].EndpointID = clientrunpod.NullableString{}
	if endpointWorkersMatch(want, got, boundedTemplateBinding(), 1000) {
		t.Fatal("worker with omitted endpointId field was accepted")
	}
	got.Workers[0].EndpointID = clientrunpod.StringValue("wrong_endpoint")
	if endpointWorkersMatch(want, got, boundedTemplateBinding(), 1000) {
		t.Fatal("worker with a wrong non-null endpointId was accepted")
	}
	got.Workers[0].EndpointID = clientrunpod.NullString()
	got.Workers[0].TemplateID = clientrunpod.StringValue("wrong_template")
	if endpointWorkersMatch(want, got, boundedTemplateBinding(), 1000) {
		t.Fatal("worker with a wrong non-null templateId was accepted")
	}
	got.Workers[0].TemplateID = clientrunpod.NullString()
	got.Workers[0].Machine.SecureCloud = nil
	if endpointWorkersMatch(want, got, boundedTemplateBinding(), 1000) {
		t.Fatal("worker with omitted secureCloud proof was accepted")
	}
	got.Workers[0].Machine.SecureCloud = ptrBool(false)
	if endpointWorkersMatch(want, got, boundedTemplateBinding(), 1000) {
		t.Fatal("Community Cloud worker was accepted")
	}
	got.Workers[0].Machine.SecureCloud = &secure

	for name, mutate := range map[string]func(*clientrunpod.EndpointWorker){
		"public IP":            func(w *clientrunpod.EndpointWorker) { w.PublicIP = clientrunpod.StringValue("203.0.113.10") },
		"port mapping":         func(w *clientrunpod.EndpointWorker) { w.PortMappings = json.RawMessage(`{"8000":18000}`) },
		"savings plan":         func(w *clientrunpod.EndpointWorker) { w.SavingsPlans = json.RawMessage(`[{"id":"plan"}]`) },
		"exited status":        func(w *clientrunpod.EndpointWorker) { w.DesiredStatus = clientrunpod.StringValue("EXITED") },
		"interruptible":        func(w *clientrunpod.EndpointWorker) { w.Interruptible = ptrBool(true) },
		"missing volume flag":  func(w *clientrunpod.EndpointWorker) { w.VolumeEncrypted = nil },
		"missing actual cost":  func(w *clientrunpod.EndpointWorker) { w.CostPerHr = clientrunpod.NullableDecimal{} },
		"cost above ceiling":   func(w *clientrunpod.EndpointWorker) { w.CostPerHr = clientrunpod.DecimalValue(1.01) },
		"machine cost missing": func(w *clientrunpod.EndpointWorker) { w.Machine.CostPerHr = clientrunpod.NullableDecimal{} },
		"machine cost ceiling": func(w *clientrunpod.EndpointWorker) { w.Machine.CostPerHr = clientrunpod.DecimalValue(1.01) },
		"price above ceiling":  func(w *clientrunpod.EndpointWorker) { w.Machine.CurrentPricePerGPU = clientrunpod.DecimalValue(1.01) },
	} {
		t.Run(name, func(t *testing.T) {
			worker := got.Workers[0]
			machine := *worker.Machine
			worker.Machine = &machine
			mutate(&worker)
			candidate := *got
			candidate.Workers = []clientrunpod.EndpointWorker{worker}
			if endpointWorkersMatch(want, &candidate, boundedTemplateBinding(), 1000) {
				t.Fatalf("worker with %s was accepted", name)
			}
		})
	}
}

func TestEndpointRequiresContemporaneousExactNestedTemplate(t *testing.T) {
	want := endpointInput(endpointCR(), "tpl_1")
	binding := boundedTemplateBinding()
	base := endpointFromInput("endpoint_1", want)
	if !endpointTemplateMatches(base, binding) {
		t.Fatal("exact nested managed Template was rejected")
	}
	for name, mutate := range map[string]func(*clientrunpod.Endpoint){
		"omitted":  func(got *clientrunpod.Endpoint) { got.Template = nil },
		"wrong ID": func(got *clientrunpod.Endpoint) { got.Template.ID = "tpl_other" },
		"wrong image": func(got *clientrunpod.Endpoint) {
			got.Template.ImageName = "registry/other@sha256:" + strings.Repeat("b", 64)
		},
		"hidden env":      func(got *clientrunpod.Endpoint) { got.Template.Env = map[string]string{"SECRET": "remote"} },
		"official RunPod": func(got *clientrunpod.Endpoint) { got.Template.IsRunpod = ptrBool(true) },
		"volume unproven": func(got *clientrunpod.Endpoint) { got.Template.VolumeInGB = nil },
		"mount unproven":  func(got *clientrunpod.Endpoint) { got.Template.VolumeMountPath = clientrunpod.NullableString{} },
		"auth unproven":   func(got *clientrunpod.Endpoint) { got.Template.ContainerRegistryAuthID = clientrunpod.NullableString{} },
	} {
		t.Run(name, func(t *testing.T) {
			got := *base
			templateCopy := *base.Template
			got.Template = &templateCopy
			mutate(&got)
			if endpointTemplateMatches(&got, binding) {
				t.Fatal("missing or mismatched nested Template was accepted")
			}
		})
	}
}

func TestUnrepairableEndpointStateFailsWithoutUpdateLoop(t *testing.T) {
	for name, mutate := range map[string]func(*clientrunpod.Endpoint){
		"compute type":                  func(got *clientrunpod.Endpoint) { got.ComputeType = "CPU" },
		"environment":                   func(got *clientrunpod.Endpoint) { got.Env = map[string]string{"SECRET": "unmanaged"} },
		"CPU instance selection on GPU": func(got *clientrunpod.Endpoint) { got.InstanceIDs = []string{"cpu3c-8-16"} },
		"network volume ID":             func(got *clientrunpod.Endpoint) { got.NetworkVolumeID = clientrunpod.StringValue("volume_1") },
		"network volume IDs":            func(got *clientrunpod.Endpoint) { got.NetworkVolumeIDs = []string{"volume_1"} },
		"minimum CUDA override":         func(got *clientrunpod.Endpoint) { got.MinCUDAVersion = clientrunpod.StringValue("13.0") },
		"unrequested CUDA allowlist":    func(got *clientrunpod.Endpoint) { got.AllowedCUDAVersions = []string{"13.0"} },
	} {
		t.Run(name, func(t *testing.T) {
			cr := endpointCR()
			bindExternalID(cr, "endpoint_1")
			got := endpointFromInput("endpoint_1", endpointInput(cr, "tpl_1"))
			mutate(got)
			svc := &fakeEndpointService{got: got}
			if _, err := (&external{service: svc, template: boundedTemplateBinding()}).Observe(context.Background(), cr); err == nil {
				t.Fatal("unrepairable external state entered normal drift reconciliation")
			}
			if svc.updates != 0 {
				t.Fatalf("unrepairable external state caused %d PATCH calls", svc.updates)
			}
		})
	}
}

func TestDeleteWaitsForEndpointCheckRouteDrain(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	cr := endpointCR()
	bindExternalID(cr, "endpoint_1")
	check := &verificationv1alpha1.EndpointCheck{ObjectMeta: metav1.ObjectMeta{Name: "check", Namespace: cr.Namespace}, Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{EndpointIDRef: &xpv2.Reference{Name: cr.Name}}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check).Build()
	svc := &fakeEndpointService{}
	if _, err := (&external{service: svc, kube: kube, reader: kube}).Delete(context.Background(), cr); err == nil || svc.deletes != 0 {
		t.Fatal("endpoint deleted before route drain")
	}
	if err := kube.Delete(context.Background(), check); err != nil {
		t.Fatal(err)
	}
	svc.got = endpointFromInput("endpoint_1", endpointInput(cr, "tpl_1"))
	if _, err := (&external{service: svc, kube: kube, reader: kube}).Delete(context.Background(), cr); err != nil || svc.deletes != 1 {
		t.Fatalf("delete err=%v calls=%d", err, svc.deletes)
	}
}

func TestDeleteUsesDirectReaderForRouteDrainGate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	cr := endpointCR()
	bindExternalID(cr, "endpoint_1")
	check := &verificationv1alpha1.EndpointCheck{
		ObjectMeta: metav1.ObjectMeta{Name: "direct-only", Namespace: cr.Namespace},
		Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{
			EndpointIDRef: &xpv2.Reference{Name: cr.Name},
		}},
	}
	staleCache := fake.NewClientBuilder().WithScheme(scheme).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check).Build()
	svc := &fakeEndpointService{}
	if _, err := (&external{service: svc, kube: staleCache, reader: direct}).Delete(context.Background(), cr); err == nil || svc.deletes != 0 {
		t.Fatal("cached EndpointCheck miss bypassed the direct route-drain gate")
	}
}

func TestResolveTemplateReferenceRequiresExternalID(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system", Generation: 1}}
	meta.SetExternalName(template, "tpl_1")
	template.Status.ObservedGeneration = 1
	template.Status.AtProvider.ID = "tpl_1"
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: template.Name}
	id, err := resolveTemplateID(context.Background(), kube, cr)
	if err != nil || id != "tpl_1" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestResolveTemplateRejectsExternalNameAnnotationTampering(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system", Generation: 1}}
	meta.SetExternalName(template, "tpl_tampered")
	template.Status.ObservedGeneration = 1
	template.Status.AtProvider.ID = "tpl_controller_observed"
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: template.Name}
	if _, err := resolveTemplateID(context.Background(), kube, cr); err == nil {
		t.Fatal("metadata-only Template external-name tampering was accepted")
	}
}

func TestResolveTemplateReferenceRequiresReadyTemplate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system"}}
	meta.SetExternalName(template, "tpl_1")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: template.Name}
	if _, err := resolveTemplateID(context.Background(), kube, cr); err == nil {
		t.Fatal("unverified Template was accepted")
	}
}

func TestConnectorResolvesTemplateThroughDirectReader(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system", Generation: 1}}
	meta.SetExternalName(template, "tpl_direct")
	template.Status.ObservedGeneration = 1
	template.Status.AtProvider.ID = "tpl_direct"
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	staleCache := fake.NewClientBuilder().WithScheme(scheme).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: template.Name}
	c := &connector{kube: staleCache, reader: direct}
	id, err := c.resolveTemplateID(context.Background(), cr)
	if err != nil || id != "tpl_direct" {
		t.Fatalf("direct Template resolution id=%q err=%v", id, err)
	}
}

func TestResolveTemplateReferenceRejectsStaleReadyGeneration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system", Generation: 2}}
	meta.SetExternalName(template, "tpl_1")
	template.Status.ObservedGeneration = 1
	template.Status.AtProvider.ID = "tpl_1"
	template.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: template.Name}
	if _, err := resolveTemplateID(context.Background(), kube, cr); err == nil {
		t.Fatal("Ready condition from a stale Template generation was accepted")
	}
}

func TestDeletingEndpointDoesNotDependOnReferencedTemplate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: "already-deleted"}
	now := metav1.Now()
	cr.DeletionTimestamp = &now

	id, err := resolveTemplateID(context.Background(), kube, cr)
	if err != nil || id != "" {
		t.Fatalf("terminating Endpoint still required Template: id=%q err=%v", id, err)
	}
}

func endpointCR() *serverlessv1alpha1.Endpoint {
	return &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "runpod-system", UID: "12345678-1234-1234-1234-123456789abc"},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{
			MaxLifetimeSeconds: 3600, Name: "experiment", TemplateIDRef: &xpv2.Reference{Name: "template"}, GPUTypeIDs: []string{"NVIDIA L4"},
			DataCenterIDs: []string{"EU-RO-1"},
			WorkersMin:    1, WorkersMax: 1, MaxWorkerCostMilliUSDPerHour: 2000, IdleTimeout: 5, ScalerType: "QUEUE_DELAY", ScalerValue: 4, ExecutionTimeoutMS: 60000,
		}},
	}
}

func endpointFromInput(id string, in clientrunpod.EndpointInput) *clientrunpod.Endpoint {
	flashBoot := in.FlashBoot
	allowedCUDA := slices.Clone(in.AllowedCUDAVersions)
	if allowedCUDA == nil {
		allowedCUDA = []string{}
	}
	volumeIDs := slices.Clone(in.NetworkVolumeIDs)
	if volumeIDs == nil {
		volumeIDs = []string{}
	}
	binding := boundedTemplateBinding()
	zero := int32(0)
	return &clientrunpod.Endpoint{
		ID: id, Name: in.Name, TemplateID: in.TemplateID, ComputeType: in.ComputeType, GPUCount: in.GPUCount,
		GPUTypeIDs: in.GPUTypeIDs, AllowedCUDAVersions: allowedCUDA, DataCenterIDs: in.DataCenterIDs,
		WorkersMin: ptrInt32(in.WorkersMin), WorkersMax: ptrInt32(in.WorkersMax), IdleTimeout: in.IdleTimeout,
		ScalerType: in.ScalerType, ScalerValue: in.ScalerValue, ExecutionTimeoutMS: in.ExecutionTimeoutMS,
		FlashBoot: &flashBoot, NetworkVolumeID: clientrunpod.StringValue(in.NetworkVolumeID), NetworkVolumeIDs: volumeIDs,
		MinCUDAVersion: clientrunpod.StringValue(in.MinCUDAVersion), Env: map[string]string{}, Version: ptrInt32(1),
		Workers: []clientrunpod.EndpointWorker{}, Template: &clientrunpod.Template{
			ID: binding.id, Name: binding.name, Category: "NVIDIA", ImageName: binding.image,
			IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true),
			ContainerDiskInGB: ptrInt32(binding.containerDiskGB), DockerEntrypoint: slices.Clone(binding.entrypoint),
			DockerStartCmd: slices.Clone(binding.startCmd), Ports: slices.Clone(binding.ports), Readme: clientrunpod.StringValue(""),
			VolumeInGB: &zero, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{},
			ContainerRegistryAuthID: clientrunpod.NullString(),
		},
	}
}

func boundedTemplateBinding() templateBinding {
	return templateBinding{id: "tpl_1", uid: "template-uid", generation: 1, image: "registry/model@sha256:" + strings.Repeat("a", 64), name: "template-external", containerDiskGB: 50, entrypoint: []string{}, startCmd: []string{}, ports: []string{"8000/http"}}
}

func ptrInt32(value int32) *int32 { return &value }
func ptrBool(value bool) *bool    { return &value }
