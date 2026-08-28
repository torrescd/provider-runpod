// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package template

import (
	"context"
	"errors"
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
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
)

type fakeService struct {
	created   int
	input     clientrunpod.TemplateInput
	createErr error
	recovered *clientrunpod.Template
	findErr   error
	findName  string
	endpoints []clientrunpod.Endpoint
	listErr   error
	deletes   int
	updates   int
	got       *clientrunpod.Template
	gets      int
}

func (f *fakeService) GetTemplate(context.Context, string) (*clientrunpod.Template, error) {
	f.gets++
	if f.got != nil {
		return f.got, nil
	}
	return nil, clientrunpod.ErrNotFound
}
func (f *fakeService) FindTemplateByName(_ context.Context, name string) (*clientrunpod.Template, error) {
	f.findName = name
	if f.findErr != nil {
		return nil, f.findErr
	}
	if f.recovered == nil {
		return nil, clientrunpod.ErrNotFound
	}
	return f.recovered, nil
}
func (f *fakeService) CreateTemplate(_ context.Context, in clientrunpod.TemplateInput) (*clientrunpod.Template, error) {
	f.created++
	f.input = in
	if f.createErr != nil {
		return nil, f.createErr
	}
	disk := in.ContainerDiskInGB
	volume := in.VolumeInGB
	return &clientrunpod.Template{ID: "tpl_1", Name: in.Name, Category: in.Category, ImageName: in.ImageName, IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk, DockerEntrypoint: in.DockerEntrypoint, DockerStartCmd: in.DockerStartCmd, Ports: in.Ports, Readme: clientrunpod.StringValue(in.Readme), VolumeInGB: &volume, VolumeMountPath: clientrunpod.StringValue(in.VolumeMountPath), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString()}, nil
}

func (f *fakeService) UpdateTemplate(context.Context, string, clientrunpod.TemplateInput) (*clientrunpod.Template, error) {
	f.updates++
	return nil, errors.New("not used")
}
func (f *fakeService) DeleteTemplate(context.Context, string) error {
	f.deletes++
	return clientrunpod.ErrNotFound
}
func (f *fakeService) ListEndpoints(context.Context) ([]clientrunpod.Endpoint, error) {
	return f.endpoints, f.listErr
}

func TestCreateRequiresDigestAndRecoversAmbiguousResult(t *testing.T) {
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "12345678-1234-1234-1234-123456789abc"}, Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{Name: "model", ImageName: "registry/model:latest", Ports: []string{"8000/http"}}}}
	svc := &fakeService{}
	e := &external{service: svc}
	if _, err := e.Create(context.Background(), cr); err == nil || svc.created != 0 {
		t.Fatal("mutable image tag was accepted")
	}
	cr.Spec.ForProvider.ImageName = "registry/model@sha256:" + strings.Repeat("a", 64)
	svc.createErr = clientrunpod.ErrCreateAmbiguous
	disk := defaultContainerDiskInGB
	volume := int32(0)
	svc.recovered = &clientrunpod.Template{ID: "tpl_recovered", Name: effectiveName(cr), Category: "NVIDIA", ImageName: cr.Spec.ForProvider.ImageName, IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk, DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"}, Readme: clientrunpod.StringValue(""), VolumeInGB: &volume, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString()}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if meta.GetExternalName(cr) != "tpl_recovered" {
		t.Fatalf("external name=%q", meta.GetExternalName(cr))
	}
	if svc.findName != effectiveName(cr) || svc.findName == cr.Spec.ForProvider.Name {
		t.Fatalf("ambiguous recovery name=%q is not CR-UID scoped", svc.findName)
	}
	if svc.input.VolumeInGB != 0 {
		t.Fatalf("persistent volume=%d, want explicit zero", svc.input.VolumeInGB)
	}
}

func TestAmbiguousCreatePersistsRecoveryAndNeverPostsTwice(t *testing.T) {
	cr := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "eventual", UID: "12345678-1234-1234-1234-123456789abc"},
		Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
			Name: "eventual", ImageName: "registry/model@sha256:" + strings.Repeat("b", 64),
			Ports: []string{"8000/http"},
		}},
	}
	svc := &fakeService{createErr: clientrunpod.ErrCreateAmbiguous}
	e := &external{service: svc}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	pendingName := cr.Annotations[ambiguousCreateNameAnnotation]
	if pendingName == "" || svc.created != 1 || meta.GetExternalName(cr) != "" {
		t.Fatalf("pending=%q creates=%d external=%q", pendingName, svc.created, meta.GetExternalName(cr))
	}
	if _, err := e.Observe(context.Background(), cr); err == nil {
		t.Fatal("not-yet-visible ambiguous create did not fail closed")
	}
	// Even if a caller invokes Create again, the durable marker makes it a
	// read-only recovery attempt and no second billable POST is issued.
	if _, err := e.Create(context.Background(), cr); err != nil || svc.created != 1 {
		t.Fatalf("second create attempt err=%v POSTs=%d", err, svc.created)
	}
	disk := defaultContainerDiskInGB
	volume := int32(0)
	svc.recovered = &clientrunpod.Template{
		ID: "tpl_eventual", Name: pendingName, ImageName: cr.Spec.ForProvider.ImageName,
		Category: "NVIDIA", IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk, DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"}, Readme: clientrunpod.StringValue(""), VolumeInGB: &volume, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
	}
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || meta.GetExternalName(cr) != "tpl_eventual" || cr.Annotations[ambiguousCreateNameAnnotation] != "" || svc.created != 1 {
		t.Fatalf("eventual recovery observation=%+v external=%q marker=%q POSTs=%d err=%v", observed, meta.GetExternalName(cr), cr.Annotations[ambiguousCreateNameAnnotation], svc.created, err)
	}
}

func TestRuntimeIncompleteTemplateCreateRecoversWithoutSecondPost(t *testing.T) {
	cr := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "12345678-1234-1234-1234-123456789abc"},
		Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
			Name: "model", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), Ports: []string{"8000/http"},
		}},
	}
	meta.SetExternalCreatePending(cr, time.Now())
	svc := &fakeService{}
	e := &external{service: svc}
	if _, err := e.Observe(context.Background(), cr); err == nil || svc.created != 0 {
		t.Fatalf("incomplete Template not-found did not fail closed: err=%v POSTs=%d", err, svc.created)
	}
	desired := templateInput(cr)
	disk, zero := desired.ContainerDiskInGB, int32(0)
	svc.recovered = &clientrunpod.Template{
		ID: "tpl_recovered", Name: effectiveName(cr), Category: "NVIDIA", ImageName: desired.ImageName,
		IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk,
		DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: desired.Ports, Readme: clientrunpod.StringValue(""),
		VolumeInGB: &zero, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
	}
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || !observed.ResourceUpToDate || svc.created != 0 || meta.GetExternalName(cr) != "tpl_recovered" {
		t.Fatalf("crash recovery observation=%+v external=%q POSTs=%d err=%v", observed, meta.GetExternalName(cr), svc.created, err)
	}
}

func TestUnverifiedExternalNameCannotAdoptByKubernetesName(t *testing.T) {
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "tpl_existing", UID: "12345678-1234-1234-1234-123456789abc"}}
	meta.SetExternalName(cr, cr.Name)
	svc := &fakeService{got: &clientrunpod.Template{ID: cr.Name}}
	if _, err := (&external{service: svc}).Observe(context.Background(), cr); err == nil || svc.gets != 0 {
		t.Fatalf("unverified external-name was queried/adopted: err=%v GETs=%d", err, svc.gets)
	}
}

func TestForgedBoundTemplateIDCannotUpdateOrDeleteArbitraryResource(t *testing.T) {
	newCR := func() *serverlessv1alpha1.Template {
		cr := &serverlessv1alpha1.Template{
			ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "runpod-system", UID: "12345678-1234-1234-1234-123456789abc"},
			Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
				MaxLifetimeSeconds: 3600, Name: "model", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64),
				Ports: []string{"8000/http"}, VolumeInGB: 0,
			}},
		}
		meta.SetExternalName(cr, "tpl_victim")
		cr.Annotations[externalIDBoundAnnotation] = "true"
		return cr
	}
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	reader := fake.NewClientBuilder().WithScheme(scheme).Build()

	cr := newCR()
	svc := &fakeService{got: &clientrunpod.Template{ID: "tpl_victim", Name: "someone-elses-template"}}
	e := &external{service: svc, reader: reader}
	if _, err := e.Observe(context.Background(), cr); err == nil || svc.gets != 1 {
		t.Fatal("forged bound marker was accepted without an exact-name GET rejection")
	}
	if _, err := e.Update(context.Background(), cr); err == nil || svc.updates != 0 {
		t.Fatalf("forged bound marker reached Template PATCH: err=%v updates=%d", err, svc.updates)
	}

	deleting := newCR()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	if _, err := e.Observe(context.Background(), deleting); err == nil || svc.gets != 2 {
		t.Fatal("deleting forged bound marker was accepted by Observe")
	}
	if _, err := e.Delete(context.Background(), deleting); err == nil || svc.deletes != 0 {
		t.Fatalf("forged bound marker reached Template DELETE: err=%v deletes=%d", err, svc.deletes)
	}

	owned := newCR()
	bindExternalID(owned, "tpl_owned")
	svc.got = &clientrunpod.Template{ID: "tpl_owned", Name: "externally-renamed"}
	if _, err := e.Delete(context.Background(), owned); err != nil || svc.deletes != 1 {
		t.Fatalf("durably bound Template could not be cleaned up after external name drift: err=%v deletes=%d", err, svc.deletes)
	}
}

func TestBoundTemplateNotFoundNeverCreatesReplacementButDeletionCompletes(t *testing.T) {
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "runpod-system", UID: "12345678-1234-1234-1234-123456789abc",
	}}
	bindExternalID(cr, "tpl_missing")
	svc := &fakeService{}
	e := &external{service: svc}
	if _, err := e.Observe(context.Background(), cr); err == nil || svc.created != 0 {
		t.Fatalf("bound 404 permitted replacement create: err=%v creates=%d", err, svc.created)
	}
	deleting := cr.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	observed, err := e.Observe(context.Background(), deleting)
	if err != nil || observed.ResourceExists || svc.created != 0 {
		t.Fatalf("deleting bound 404 did not converge: observation=%+v err=%v creates=%d", observed, err, svc.created)
	}
}

func TestCreateCriticalAnnotationBoundaryRepopulatesTemplateStatus(t *testing.T) {
	cr := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "12345678-1234-1234-1234-123456789abc"},
		Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
			Name: "model", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), Ports: []string{"8000/http"},
		}},
	}
	svc := &fakeService{}
	e := &external{service: svc}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	id := meta.GetExternalName(cr)
	cr.Status = serverlessv1alpha1.TemplateStatus{}
	disk, zero := int32(50), int32(0)
	svc.got = &clientrunpod.Template{
		ID: id, Name: effectiveName(cr), Category: "NVIDIA", ImageName: cr.Spec.ForProvider.ImageName,
		IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk,
		DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"}, Readme: clientrunpod.StringValue(""),
		VolumeInGB: &zero, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
	}
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || !observed.ResourceUpToDate || cr.Status.AtProvider.ID != id {
		t.Fatalf("critical-annotation recovery observation=%+v status=%+v err=%v", observed, cr.Status.AtProvider, err)
	}
}

func TestManagedReconcilerPersistsAmbiguousTemplateRecoveryBinding(t *testing.T) {
	cr := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "runpod-system", UID: "12345678-1234-1234-1234-123456789abc",
			Annotations: map[string]string{}},
		Spec: serverlessv1alpha1.TemplateSpec{ManagedResourceSpec: xpv2.ManagedResourceSpec{ManagementPolicies: xpv2.ManagementPolicies{xpv2.ManagementActionAll}}, ForProvider: serverlessv1alpha1.TemplateParameters{
			Name: "model", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), Ports: []string{"8000/http"},
		}},
	}
	markAmbiguousCreate(cr, effectiveName(cr))
	desired := templateInput(cr)
	disk, zero := desired.ContainerDiskInGB, int32(0)
	svc := &fakeService{recovered: &clientrunpod.Template{
		ID: "tpl_recovered", Name: effectiveName(cr), Category: "NVIDIA", ImageName: desired.ImageName,
		IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk,
		DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: desired.Ports, Readme: clientrunpod.StringValue(""),
		VolumeInGB: &zero, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
	}}
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr).Build()
	mgr := &resourcefake.Manager{Client: kube, Scheme: scheme}
	ext := &external{service: svc}
	r := managed.NewReconciler(mgr, resource.ManagedKind(serverlessv1alpha1.TemplateGroupVersionKind),
		managed.WithInitializers(),
		managed.WithDeterministicExternalName(true),
		managed.WithManagementPolicies(),
		managed.WithFinalizer(resource.FinalizerFns{AddFinalizerFn: func(context.Context, resource.Object) error { return nil }}),
		managed.WithTypedExternalConnector(managed.TypedExternalConnectorFn[*serverlessv1alpha1.Template](func(context.Context, *serverlessv1alpha1.Template) (managed.TypedExternalClient[*serverlessv1alpha1.Template], error) {
			return ext, nil
		})),
	)
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}); err != nil {
		t.Fatal(err)
	}
	persisted := &serverlessv1alpha1.Template{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}, persisted); err != nil {
		t.Fatal(err)
	}
	if meta.GetExternalName(persisted) != "tpl_recovered" || persisted.Annotations[externalIDBoundAnnotation] != "true" || persisted.Annotations[ambiguousCreateNameAnnotation] != "" {
		t.Fatalf("managed reconcile did not persist recovered metadata: %#v", persisted.Annotations)
	}
}

func TestManagedReconcilerRetainsAmbiguousTemplateUntilLateVisibility(t *testing.T) {
	deletion := metav1.NewTime(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	cr := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{
			Name: "late-template", Namespace: "runpod-system", UID: "12345678-1234-1234-1234-123456789abc",
			DeletionTimestamp: &deletion, Finalizers: []string{"finalizer.managedresource.crossplane.io"}, Annotations: map[string]string{},
		},
		Spec: serverlessv1alpha1.TemplateSpec{ManagedResourceSpec: xpv2.ManagedResourceSpec{ManagementPolicies: xpv2.ManagementPolicies{xpv2.ManagementActionAll}}, ForProvider: serverlessv1alpha1.TemplateParameters{
			Name: "late-template", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), Ports: []string{"8000/http"},
		}},
	}
	meta.SetExternalCreatePending(cr, deletion.Time.Add(-time.Minute))
	markAmbiguousCreate(cr, effectiveName(cr))
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cr).WithObjects(cr).Build()
	svc := &fakeService{}
	ext := &external{service: svc, reader: kube}
	removed := 0
	r := managed.NewReconciler(&resourcefake.Manager{Client: kube, Scheme: scheme}, resource.ManagedKind(serverlessv1alpha1.TemplateGroupVersionKind),
		managed.WithInitializers(), managed.WithDeterministicExternalName(true), managed.WithManagementPolicies(),
		managed.WithFinalizer(resource.FinalizerFns{
			AddFinalizerFn:    func(context.Context, resource.Object) error { return nil },
			RemoveFinalizerFn: func(context.Context, resource.Object) error { removed++; return nil },
		}),
		managed.WithTypedExternalConnector(managed.TypedExternalConnectorFn[*serverlessv1alpha1.Template](func(context.Context, *serverlessv1alpha1.Template) (managed.TypedExternalClient[*serverlessv1alpha1.Template], error) {
			return ext, nil
		})),
	)
	request := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: cr.Namespace, Name: cr.Name}}
	_, _ = r.Reconcile(context.Background(), request)
	if removed != 0 || svc.deletes != 0 {
		t.Fatalf("one absent list read released ambiguous Template: finalizer removals=%d deletes=%d", removed, svc.deletes)
	}

	disk, zero := int32(50), int32(0)
	svc.recovered = &clientrunpod.Template{
		ID: "tpl_late", Name: effectiveName(cr), Category: "AMD", ImageName: cr.Spec.ForProvider.ImageName,
		IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk,
		DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"}, Readme: clientrunpod.StringValue(""),
		VolumeInGB: &zero, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
	}
	svc.got = svc.recovered
	for i := 0; i < 4 && svc.deletes == 0; i++ {
		_, _ = r.Reconcile(context.Background(), request)
	}
	if svc.deletes != 1 {
		t.Fatalf("late-visible ambiguous Template was not recovered and deleted: deletes=%d removals=%d", svc.deletes, removed)
	}
}

func TestDeletingAmbiguousTemplateAdoptsOnlyForSafeCleanup(t *testing.T) {
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{
		Name: "model", Namespace: "runpod-system", UID: "12345678-1234-1234-1234-123456789abc",
	}}
	now := metav1.Now()
	cr.DeletionTimestamp = &now
	markAmbiguousCreate(cr, effectiveName(cr))
	// Deliberate drift is irrelevant during cleanup; exact UID-scoped unique
	// name and a safe external ID are sufficient to recover deletion.
	svc := &fakeService{recovered: &clientrunpod.Template{ID: "tpl_cleanup", Name: effectiveName(cr), Category: "AMD"}}
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	e := &external{service: svc, reader: fake.NewClientBuilder().WithScheme(scheme).Build()}
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceExists || meta.GetExternalName(cr) != "tpl_cleanup" {
		t.Fatalf("cleanup recovery observation=%+v external=%q err=%v", observed, meta.GetExternalName(cr), err)
	}
	svc.got = svc.recovered
	if _, err := e.Delete(context.Background(), cr); err != nil || svc.deletes != 1 {
		t.Fatalf("cleanup delete err=%v calls=%d", err, svc.deletes)
	}

	cr2 := cr.DeepCopy()
	cr2.Annotations = nil
	meta.SetExternalName(cr2, "")
	markAmbiguousCreate(cr2, effectiveName(cr2))
	ambiguous := &fakeService{findErr: clientrunpod.ErrAmbiguous}
	e.service = ambiguous
	if _, err := e.Observe(context.Background(), cr2); err == nil || meta.GetExternalName(cr2) != "" || ambiguous.deletes != 0 {
		t.Fatal("ambiguous cleanup Template was adopted or deleted")
	}

	absent := cr.DeepCopy()
	absent.Name = "absence-confirmation"
	absent.UID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	absent.Annotations = nil
	absent.Status = serverlessv1alpha1.TemplateStatus{}
	meta.SetExternalName(absent, "")
	meta.SetExternalCreatePending(absent, time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	markAmbiguousCreate(absent, effectiveName(absent))
	absenceService := &fakeService{}
	e = &external{service: absenceService, reader: fake.NewClientBuilder().WithScheme(scheme).Build()}
	if observed, err = e.Observe(context.Background(), absent); err == nil || observed.ResourceExists {
		t.Fatalf("unacknowledged ambiguous absence completed deletion: observation=%+v err=%v", observed, err)
	}
	absent.Annotations[ambiguousAbsenceConfirmedAnnotation] = "wrong-pending-token"
	if observed, err = e.Observe(context.Background(), absent); err == nil || observed.ResourceExists {
		t.Fatalf("mismatched absence acknowledgement completed deletion: observation=%+v err=%v", observed, err)
	}

	// A response-uncertain POST may become visible arbitrarily late. With no
	// valid operator acknowledgement the finalizer remains, allowing exact-name
	// recovery solely for cleanup whenever that happens.
	absenceService.recovered = &clientrunpod.Template{ID: "tpl_late", Name: effectiveName(absent), Category: "AMD"}
	observed, err = e.Observe(context.Background(), absent)
	if err != nil || !observed.ResourceExists || meta.GetExternalName(absent) != "tpl_late" {
		t.Fatalf("late-visible Template was not recovered for cleanup: observation=%+v err=%v", observed, err)
	}

	confirmed := cr.DeepCopy()
	confirmed.Name = "confirmed-absent"
	confirmed.UID = "ffffffff-1111-2222-3333-444444444444"
	confirmed.Annotations = nil
	confirmed.Status = serverlessv1alpha1.TemplateStatus{}
	meta.SetExternalName(confirmed, "")
	meta.SetExternalCreatePending(confirmed, time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC))
	markAmbiguousCreate(confirmed, effectiveName(confirmed))
	pendingToken := confirmed.Annotations[externalCreatePendingAnnotation]
	confirmed.Annotations[ambiguousAbsenceConfirmedAnnotation] = pendingToken
	e.service = &fakeService{}
	observed, err = e.Observe(context.Background(), confirmed)
	if err != nil || observed.ResourceExists {
		t.Fatalf("exact operator acknowledgement did not finalize deleting absence: observation=%+v err=%v", observed, err)
	}

	live := confirmed.DeepCopy()
	live.DeletionTimestamp = nil
	if _, err := e.Observe(context.Background(), live); err == nil {
		t.Fatal("live acknowledged Template resumed reconciliation")
	}
	if _, err := e.Create(context.Background(), live); err == nil || e.service.(*fakeService).created != 0 {
		t.Fatalf("live acknowledgement permitted Template recreation: err=%v", err)
	}
}

func TestTemplateVolumeRequiresObservedExplicitZero(t *testing.T) {
	disk := defaultContainerDiskInGB
	want := clientrunpod.TemplateInput{Name: "model", Category: "NVIDIA", ImageName: "image", IsServerless: true, ContainerDiskInGB: disk, VolumeInGB: 0}
	got := &clientrunpod.Template{Name: "model", Category: "NVIDIA", ImageName: "image", IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk, DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{}, Readme: clientrunpod.StringValue(""), VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString()}
	if templateMatchesInput(want, got) {
		t.Fatal("omitted volume response was accepted as proof of zero")
	}
	zero := int32(0)
	got.VolumeInGB = &zero
	if !templateMatchesInput(want, got) {
		t.Fatal("explicit observed zero volume was rejected")
	}
	volume := int32(20)
	got.VolumeInGB = &volume
	if templateMatchesInput(want, got) {
		t.Fatal("RunPod's persistent-volume default was accepted as desired state")
	}
}

func TestTemplateSecretAndCommandDriftFailsClosed(t *testing.T) {
	disk := defaultContainerDiskInGB
	want := clientrunpod.TemplateInput{Name: "model", Category: "NVIDIA", ImageName: "image", IsServerless: true, ContainerDiskInGB: disk, Ports: []string{"8000/http"}, Env: map[string]string{}}
	zero := int32(0)
	base := clientrunpod.Template{Name: "model", Category: "NVIDIA", ImageName: "image", IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk, DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"}, Readme: clientrunpod.StringValue(""), VolumeInGB: &zero, VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString()}
	got := base
	got.Env = map[string]string{"REMOTE_SECRET": "must-not-be-adopted"}
	if templateMatchesInput(want, &got) {
		t.Fatal("Template with unmanaged remote environment was accepted")
	}
	got = base
	got.ContainerRegistryAuthID = clientrunpod.StringValue("remote-registry-secret")
	if templateMatchesInput(want, &got) {
		t.Fatal("Template with unmanaged registry credentials was accepted")
	}
	got = base
	got.DockerStartCmd = []string{"unexpected", "override"}
	if templateMatchesInput(want, &got) {
		t.Fatal("Template with an unmanaged command override was accepted")
	}
	got = base
	wrongDisk := int32(51)
	got.ContainerDiskInGB = &wrongDisk
	if templateMatchesInput(want, &got) {
		t.Fatal("Template with an unmanaged container disk size was accepted")
	}
}

func TestTemplateOwnershipBooleansRequireExplicitSafeValues(t *testing.T) {
	disk, zero := int32(50), int32(0)
	want := clientrunpod.TemplateInput{
		Name: "model", Category: "NVIDIA", ImageName: "image", IsServerless: true, ContainerDiskInGB: disk,
		DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"},
		Readme: "", VolumeInGB: 0, VolumeMountPath: "", Env: map[string]string{},
	}
	valid := clientrunpod.Template{
		Name: want.Name, Category: want.Category, ImageName: want.ImageName,
		IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: &disk,
		DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: []string{"8000/http"},
		Readme: clientrunpod.StringValue(""), VolumeInGB: &zero, VolumeMountPath: clientrunpod.StringValue(""),
		Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
	}
	if !templateMatchesInput(want, &valid) {
		t.Fatal("explicit private, user-owned Serverless Template was rejected")
	}
	for name, mutate := range map[string]func(*clientrunpod.Template){
		"isPublic omitted or null": func(got *clientrunpod.Template) { got.IsPublic = nil },
		"isPublic true":            func(got *clientrunpod.Template) { got.IsPublic = ptrBool(true) },
		"isRunpod omitted or null": func(got *clientrunpod.Template) { got.IsRunpod = nil },
		"isRunpod true":            func(got *clientrunpod.Template) { got.IsRunpod = ptrBool(true) },
		"isServerless omitted":     func(got *clientrunpod.Template) { got.IsServerless = nil },
	} {
		t.Run(name, func(t *testing.T) {
			got := valid
			mutate(&got)
			if templateMatchesInput(want, &got) {
				t.Fatal("missing or unsafe ownership classification was accepted")
			}
		})
	}
}

func TestUnrepairableTemplateStateFailsWithoutUpdateLoop(t *testing.T) {
	for name, mutate := range map[string]func(*clientrunpod.Template){
		"category":       func(got *clientrunpod.Template) { got.Category = "AMD" },
		"not serverless": func(got *clientrunpod.Template) { got.IsServerless = ptrBool(false) },
		"RunPod managed": func(got *clientrunpod.Template) { got.IsRunpod = ptrBool(true) },
		"owner omitted":  func(got *clientrunpod.Template) { got.IsRunpod = nil },
	} {
		t.Run(name, func(t *testing.T) {
			cr := &serverlessv1alpha1.Template{
				ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "12345678-1234-1234-1234-123456789abc"},
				Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
					Name: "model", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), Ports: []string{"8000/http"},
				}},
			}
			bindExternalID(cr, "tpl_1")
			want := templateInput(cr)
			got := &clientrunpod.Template{
				ID: "tpl_1", Name: want.Name, Category: "NVIDIA", ImageName: want.ImageName,
				IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: ptrInt32(want.ContainerDiskInGB), DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: want.Ports, Readme: clientrunpod.StringValue(""), VolumeInGB: ptrInt32(0), VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
			}
			mutate(got)
			svc := &fakeService{got: got}
			if _, err := (&external{service: svc}).Observe(context.Background(), cr); err == nil {
				t.Fatal("unrepairable Template state entered normal drift reconciliation")
			}
			if svc.updates != 0 {
				t.Fatalf("unrepairable Template state caused %d PATCH calls", svc.updates)
			}
		})
	}
}

func TestObserveAdvancesGenerationOnlyForConfirmedCurrentTemplate(t *testing.T) {
	cr := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "model", UID: "12345678-1234-1234-1234-123456789abc", Generation: 1},
		Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
			Name: "model", ImageName: "registry/model@sha256:" + strings.Repeat("c", 64),
			Ports: []string{"8000/http"}, VolumeInGB: 0,
		}},
	}
	bindExternalID(cr, "tpl_1")
	want := templateInput(cr)
	svc := &fakeService{got: &clientrunpod.Template{
		ID: "tpl_1", Name: want.Name, ImageName: want.ImageName,
		Category: "NVIDIA", IsPublic: ptrBool(false), IsRunpod: ptrBool(false), IsServerless: ptrBool(true), ContainerDiskInGB: ptrInt32(want.ContainerDiskInGB), DockerEntrypoint: []string{}, DockerStartCmd: []string{}, Ports: want.Ports, Readme: clientrunpod.StringValue(""), VolumeInGB: ptrInt32(0), VolumeMountPath: clientrunpod.StringValue(""), Env: map[string]string{}, ContainerRegistryAuthID: clientrunpod.NullString(),
	}}
	e := &external{service: svc}
	observed, err := e.Observe(context.Background(), cr)
	if err != nil || !observed.ResourceUpToDate || cr.Status.ObservedGeneration != cr.Generation {
		t.Fatalf("initial Observe did not publish current generation: observation=%+v status=%+v err=%v", observed, cr.Status, err)
	}
	svc.got.ImageName = "registry/model@sha256:" + strings.Repeat("d", 64)
	observed, err = e.Observe(context.Background(), cr)
	if err != nil || observed.ResourceUpToDate || cr.Status.ObservedGeneration != 0 {
		t.Fatalf("drift retained a consumable generation: observation=%+v status=%+v err=%v", observed, cr.Status, err)
	}
}

func TestExpiredTemplateNeverCreatesOrUpdatesExternally(t *testing.T) {
	cr := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "expired", UID: "12345678-1234-1234-1234-123456789abc", CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour))},
		Spec: serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{
			MaxLifetimeSeconds: 3600, Name: "expired", ImageName: "registry/model@sha256:" + strings.Repeat("a", 64), Ports: []string{"8000/http"},
		}},
	}
	svc := &fakeService{}
	e := &external{service: svc}
	if _, err := e.Create(context.Background(), cr); err == nil {
		t.Fatal("expired Template create was accepted")
	}
	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("expired Template update was accepted")
	}
	if svc.created != 0 || svc.updates != 0 {
		t.Fatalf("expired Template made external calls: creates=%d updates=%d", svc.created, svc.updates)
	}
}

func ptrInt32(value int32) *int32 { return &value }
func ptrBool(value bool) *bool    { return &value }

func TestDelete404IsSuccess(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	reader := fake.NewClientBuilder().WithScheme(scheme).Build()
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system"}}
	bindExternalID(cr, "gone")
	if _, err := (&external{service: &fakeService{}, reader: reader}).Delete(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteWaitsForKubernetesAndExternalEndpointReferences(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system"}}
	bindExternalID(cr, "tpl_1")
	now := metav1.Now()
	endpoint := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name: "terminating-endpoint", Namespace: cr.Namespace,
			DeletionTimestamp: &now, Finalizers: []string{"finalizer.managedresource.crossplane.io"},
		},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{
			TemplateIDRef: &xpv2.Reference{Name: cr.Name},
		}},
	}
	// This reader represents the uncached APIReader. A stale manager cache with
	// no Endpoint cannot bypass the external delete gate because it is not used.
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(endpoint).Build()
	svc := &fakeService{}
	e := &external{service: svc, reader: direct}
	if _, err := e.Delete(context.Background(), cr); err == nil || svc.deletes != 0 {
		t.Fatal("Template deletion ignored a terminating Endpoint CR reference")
	}
	direct = fake.NewClientBuilder().WithScheme(scheme).Build()
	e.reader = direct
	svc.endpoints = []clientrunpod.Endpoint{{ID: "unmanaged", TemplateID: "tpl_1"}}
	if _, err := e.Delete(context.Background(), cr); err == nil || svc.deletes != 0 {
		t.Fatal("Template deletion ignored an authoritative external Endpoint reference")
	}
	svc.endpoints = nil
	svc.got = &clientrunpod.Template{ID: "tpl_1", Name: effectiveName(cr)}
	if _, err := e.Delete(context.Background(), cr); err != nil || svc.deletes != 1 {
		t.Fatalf("safe Template deletion err=%v calls=%d", err, svc.deletes)
	}
}

func TestDeleteFailsClosedOnSparseExternalEndpointBinding(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	cr := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system"}}
	bindExternalID(cr, "tpl_1")
	svc := &fakeService{endpoints: []clientrunpod.Endpoint{{ID: "endpoint_1", TemplateID: ""}}}
	e := &external{service: svc, reader: fake.NewClientBuilder().WithScheme(scheme).Build()}
	if _, err := e.Delete(context.Background(), cr); err == nil || svc.deletes != 0 {
		t.Fatal("sparse Endpoint list item allowed referenced Template deletion")
	}
}
