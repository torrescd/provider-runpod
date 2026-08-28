// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpoint

import (
	"context"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
)

type fakeEndpointService struct {
	created clientrunpod.EndpointInput
	deletes int
}

func (f *fakeEndpointService) GetEndpoint(context.Context, string) (*clientrunpod.Endpoint, error) {
	return nil, clientrunpod.ErrNotFound
}
func (f *fakeEndpointService) FindEndpointByName(context.Context, string) (*clientrunpod.Endpoint, error) {
	return nil, clientrunpod.ErrNotFound
}
func (f *fakeEndpointService) CreateEndpoint(_ context.Context, in clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error) {
	f.created = in
	return endpointFromInput("endpoint_1", in), nil
}
func (f *fakeEndpointService) UpdateEndpoint(_ context.Context, id string, in clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error) {
	return endpointFromInput(id, in), nil
}
func (f *fakeEndpointService) DeleteEndpoint(context.Context, string) error { f.deletes++; return nil }

func TestCreateEnforcesCostBoundsAndUniqueRecoveryName(t *testing.T) {
	cr := endpointCR()
	svc := &fakeEndpointService{}
	e := &external{service: svc, templateID: "tpl_1"}
	if _, err := e.Create(context.Background(), cr); err != nil {
		t.Fatal(err)
	}
	if svc.created.WorkersMin != 0 || svc.created.WorkersMax != 1 || svc.created.GPUCount != 1 || svc.created.ComputeType != "GPU" {
		t.Fatalf("unsafe endpoint request: %+v", svc.created)
	}
	if svc.created.Name == cr.Spec.ForProvider.Name {
		t.Fatal("endpoint recovery name was not UID-scoped")
	}
	if meta.GetExternalName(cr) != "endpoint_1" {
		t.Fatal("external ID not recorded")
	}
}

func TestDeleteWaitsForEndpointCheckRouteDrain(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	cr := endpointCR()
	meta.SetExternalName(cr, "endpoint_1")
	check := &verificationv1alpha1.EndpointCheck{ObjectMeta: metav1.ObjectMeta{Name: "check", Namespace: cr.Namespace}, Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{EndpointIDRef: &xpv2.Reference{Name: cr.Name}}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check).Build()
	svc := &fakeEndpointService{}
	if _, err := (&external{service: svc, kube: kube}).Delete(context.Background(), cr); err == nil || svc.deletes != 0 {
		t.Fatal("endpoint deleted before route drain")
	}
	if err := kube.Delete(context.Background(), check); err != nil {
		t.Fatal(err)
	}
	if _, err := (&external{service: svc, kube: kube}).Delete(context.Background(), cr); err != nil || svc.deletes != 1 {
		t.Fatalf("delete err=%v calls=%d", err, svc.deletes)
	}
}

func TestResolveTemplateReferenceRequiresExternalID(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system"}}
	meta.SetExternalName(template, "tpl_1")
	template.Status.SetConditions(xpv2.Available())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateID = ""
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: template.Name}
	id, err := resolveTemplateID(context.Background(), kube, cr)
	if err != nil || id != "tpl_1" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestResolveTemplateReferenceRequiresReadyTemplate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system"}}
	meta.SetExternalName(template, "tpl_1")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateID = ""
	cr.Spec.ForProvider.TemplateIDRef = &xpv2.Reference{Name: template.Name}
	if _, err := resolveTemplateID(context.Background(), kube, cr); err == nil {
		t.Fatal("unverified Template was accepted")
	}
}

func TestResolveTemplateReferenceRejectsStaleReadyGeneration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "template", Namespace: "runpod-system", Generation: 2}}
	meta.SetExternalName(template, "tpl_1")
	template.Status.ObservedGeneration = 1
	template.Status.SetConditions(xpv2.Available())
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	cr := endpointCR()
	cr.Spec.ForProvider.TemplateID = ""
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
	cr.Spec.ForProvider.TemplateID = ""
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
			MaxLifetimeSeconds: 3600, Name: "experiment", TemplateID: "tpl_1", GPUTypeIDs: []string{"NVIDIA L4"},
			WorkersMax: 1, IdleTimeout: 5, ScalerType: "QUEUE_DELAY", ScalerValue: 4, ExecutionTimeoutMS: 60000,
		}},
	}
}

func endpointFromInput(id string, in clientrunpod.EndpointInput) *clientrunpod.Endpoint {
	return &clientrunpod.Endpoint{ID: id, Name: in.Name, TemplateID: in.TemplateID, ComputeType: in.ComputeType, GPUCount: in.GPUCount, GPUTypeIDs: in.GPUTypeIDs, AllowedCUDAVersions: in.AllowedCUDAVersions, DataCenterIDs: in.DataCenterIDs, WorkersMin: in.WorkersMin, WorkersMax: in.WorkersMax, IdleTimeout: in.IdleTimeout, ScalerType: in.ScalerType, ScalerValue: in.ScalerValue, ExecutionTimeoutMS: in.ExecutionTimeoutMS}
}
