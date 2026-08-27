// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package janitor

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
)

func TestExpiredEndpointWithdrawsChecksBeforeDeletingInfrastructure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "runpod-system"}}
	ep := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "runpod-system", CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour))},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{
			MaxLifetimeSeconds: 3600, TemplateIDRef: &xpv2.Reference{Name: template.Name},
		}},
		Status: serverlessv1alpha1.EndpointStatus{AtProvider: serverlessv1alpha1.EndpointObservation{ID: "endpoint_1"}},
	}
	check := &verificationv1alpha1.EndpointCheck{
		ObjectMeta: metav1.ObjectMeta{Name: "check", Namespace: "runpod-system", Finalizers: []string{"router.runpod.crossplane.io/drain"}},
		Spec:       verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{EndpointIDRef: &xpv2.Reference{Name: ep.Name}}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, ep, check).Build()
	r := &reconciler{kube: kube, now: time.Now}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ep.Namespace, Name: ep.Name}}
	result, err := r.Reconcile(context.Background(), req)
	if err != nil || result.RequeueAfter != cleanupPoll {
		t.Fatalf("first reconcile=%+v err=%v", result, err)
	}
	stillThere := &serverlessv1alpha1.Endpoint{}
	if err := kube.Get(context.Background(), req.NamespacedName, stillThere); err != nil {
		t.Fatal("endpoint deleted before route drain")
	}
	deletingCheck := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), types.NamespacedName{Namespace: check.Namespace, Name: check.Name}, deletingCheck); err != nil {
		t.Fatal(err)
	}
	if deletingCheck.DeletionTimestamp.IsZero() {
		t.Fatal("check was not marked for deletion")
	}

	deletingCheck.Finalizers = nil
	if err := kube.Update(context.Background(), deletingCheck); err != nil {
		t.Fatal(err)
	}
	_, err = r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range []client.Object{&serverlessv1alpha1.Endpoint{}, &serverlessv1alpha1.Template{}} {
		key := req.NamespacedName
		if _, ok := object.(*serverlessv1alpha1.Template); ok {
			key.Name = template.Name
		}
		if err := kube.Get(context.Background(), key, object); err == nil {
			t.Fatalf("%T was not deleted", object)
		}
	}
}

func TestUnexpiredEndpointSchedulesAtDeadline(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	now := time.Now()
	ep := &serverlessv1alpha1.Endpoint{ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "runpod-system", CreationTimestamp: metav1.NewTime(now)}, Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{MaxLifetimeSeconds: 3600}}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ep).Build()
	r := &reconciler{kube: kube, now: func() time.Time { return now }}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ep.Namespace, Name: ep.Name}})
	// Kubernetes timestamps are persisted at second precision, so allow for the
	// sub-second component dropped by the fake API client round trip.
	if err != nil || result.RequeueAfter <= 59*time.Minute || result.RequeueAfter > time.Hour {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
