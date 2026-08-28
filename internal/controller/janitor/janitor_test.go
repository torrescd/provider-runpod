// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package janitor

import (
	"context"
	"testing"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/gate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
)

func TestSetupGatedWaitsForAllRequiredCRDs(t *testing.T) {
	original := setupController
	defer func() { setupController = original }()
	called := make(chan struct{})
	setupController = func(ctrl.Manager) error {
		close(called)
		return nil
	}

	g := new(gate.Gate[schema.GroupVersionKind])
	if err := SetupGated(nil, controller.Options{Gate: g}); err != nil {
		t.Fatal(err)
	}
	g.Set(serverlessv1alpha1.EndpointGroupVersionKind, true)
	select {
	case <-called:
		t.Fatal("janitor started before EndpointCheck CRD was established")
	case <-time.After(20 * time.Millisecond):
	}
	g.Set(verificationv1alpha1.EndpointCheckGroupVersionKind, true)
	select {
	case <-called:
		t.Fatal("janitor started before Template CRD was established")
	case <-time.After(20 * time.Millisecond):
	}
	g.Set(serverlessv1alpha1.TemplateGroupVersionKind, true)
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("janitor did not start after all required CRDs were established")
	}
}

func TestExpiryUsesOneClockSampleAndExactBoundaryIsExpired(t *testing.T) {
	for name, tc := range map[string]struct {
		firstNow    time.Duration
		wantExpired bool
	}{
		"crossing after sample waits": {firstNow: -time.Nanosecond, wantExpired: false},
		"exact boundary expires":      {firstNow: 0, wantExpired: true},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
			_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
			boundary := time.Now().Round(time.Second)
			template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "runpod-system"}}
			ep := &serverlessv1alpha1.Endpoint{
				ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "runpod-system", CreationTimestamp: metav1.NewTime(boundary.Add(-time.Hour)), Finalizers: []string{lifetimeFinalizer, "finalizer.managedresource.crossplane.io"}},
				Spec:       serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{MaxLifetimeSeconds: 3600, TemplateIDRef: &xpv2.Reference{Name: template.Name}}},
			}
			kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, ep).Build()
			calls := 0
			r := &reconciler{kube: kube, reader: kube, now: func() time.Time {
				calls++
				if calls == 1 {
					return boundary.Add(tc.firstNow)
				}
				return boundary.Add(time.Nanosecond)
			}}
			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ep)})
			if err != nil || calls != 1 {
				t.Fatalf("result=%+v err=%v clock calls=%d", result, err, calls)
			}
			gotTemplate := &serverlessv1alpha1.Template{}
			if err := kube.Get(context.Background(), client.ObjectKeyFromObject(template), gotTemplate); err != nil {
				t.Fatal(err)
			}
			marked := gotTemplate.Annotations[templateReapAnnotation] != ""
			if marked != tc.wantExpired {
				t.Fatalf("template marked=%v, want expired=%v result=%+v", marked, tc.wantExpired, result)
			}
			if !tc.wantExpired && result.RequeueAfter != time.Nanosecond {
				t.Fatalf("pre-boundary requeue=%s, want 1ns", result.RequeueAfter)
			}
		})
	}
}

func TestExpiredEndpointWithdrawsChecksBeforeDeletingInfrastructure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "runpod-system"}}
	ep := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name: "endpoint", Namespace: "runpod-system", CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
			Finalizers: []string{lifetimeFinalizer, "finalizer.managedresource.crossplane.io"},
		},
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
	r := &reconciler{kube: kube, reader: kube, now: time.Now}
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
	if _, err = r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	deletingEndpoint := &serverlessv1alpha1.Endpoint{}
	if err := kube.Get(context.Background(), req.NamespacedName, deletingEndpoint); err != nil {
		t.Fatal("endpoint disappeared before external-deletion finalizer completed")
	}
	if deletingEndpoint.DeletionTimestamp.IsZero() {
		t.Fatal("endpoint was not marked for deletion after route drain")
	}
	markedTemplate := &serverlessv1alpha1.Template{}
	templateKey := types.NamespacedName{Namespace: template.Namespace, Name: template.Name}
	if err := kube.Get(context.Background(), templateKey, markedTemplate); err != nil || markedTemplate.Annotations[templateReapAnnotation] == "" {
		t.Fatalf("template cleanup intent was not recorded before releasing Endpoint: template=%+v err=%v", markedTemplate, err)
	}

	// Crossplane-runtime v2 refuses ExternalDelete while more than its own
	// finalizer remains. The next reconciliation must release the janitor lock,
	// not wait for Crossplane and deadlock.
	if len(deletingEndpoint.Finalizers) != 2 {
		t.Fatalf("precondition finalizers=%v, want janitor plus managed", deletingEndpoint.Finalizers)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), req.NamespacedName, deletingEndpoint); err != nil {
		t.Fatal(err)
	}
	if contains(deletingEndpoint.Finalizers, lifetimeFinalizer) || len(deletingEndpoint.Finalizers) != 1 {
		t.Fatalf("janitor did not release Crossplane deletion: %v", deletingEndpoint.Finalizers)
	}
	if _, err := (&templateReconciler{kube: kube, reader: kube}).Reconcile(context.Background(), ctrl.Request{NamespacedName: templateKey}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), templateKey, &serverlessv1alpha1.Template{}); err != nil {
		t.Fatal("template deleted while Endpoint CR still proved external cleanup incomplete")
	}

	// Simulate Crossplane removing its own finalizer only after ExternalDelete.
	deletingEndpoint.Finalizers = nil
	if err := kube.Update(context.Background(), deletingEndpoint); err != nil {
		t.Fatal(err)
	}
	if _, err := (&templateReconciler{kube: kube, reader: kube}).Reconcile(context.Background(), ctrl.Request{NamespacedName: templateKey}); err != nil {
		t.Fatal(err)
	}
	for _, object := range []client.Object{&serverlessv1alpha1.Endpoint{}, &serverlessv1alpha1.Template{}} {
		key := req.NamespacedName
		if _, ok := object.(*serverlessv1alpha1.Template); ok {
			key = templateKey
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
	r := &reconciler{kube: kube, reader: kube, now: func() time.Time { return now }}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ep.Namespace, Name: ep.Name}}
	if _, err := r.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	result, err := r.Reconcile(context.Background(), request)
	// Kubernetes timestamps are persisted at second precision, so allow for the
	// sub-second component dropped by the fake API client round trip.
	if err != nil || result.RequeueAfter <= 59*time.Minute || result.RequeueAfter > time.Hour {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestManualDeletionDrainsRouteThenReleasesJanitorWithoutTemplateReap(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "runpod-system"}}
	ep := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name: "endpoint", Namespace: "runpod-system", CreationTimestamp: metav1.Now(),
			Finalizers: []string{lifetimeFinalizer, "finalizer.managedresource.crossplane.io"},
		},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{
			MaxLifetimeSeconds: 3600, TemplateIDRef: &xpv2.Reference{Name: template.Name},
		}},
	}
	check := &verificationv1alpha1.EndpointCheck{
		ObjectMeta: metav1.ObjectMeta{Name: "check", Namespace: ep.Namespace, Finalizers: []string{"router.runpod.crossplane.io/drain"}},
		Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{
			EndpointIDRef: &xpv2.Reference{Name: ep.Name},
		}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, ep, check).Build()
	if err := kube.Delete(context.Background(), ep); err != nil {
		t.Fatal(err)
	}
	r := &reconciler{kube: kube, reader: kube, now: time.Now}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ep)}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	deletingCheck := &verificationv1alpha1.EndpointCheck{}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(check), deletingCheck); err != nil {
		t.Fatal(err)
	}
	deletingCheck.Finalizers = nil
	if err := kube.Update(context.Background(), deletingCheck); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	deletingEndpoint := &serverlessv1alpha1.Endpoint{}
	if err := kube.Get(context.Background(), req.NamespacedName, deletingEndpoint); err != nil {
		t.Fatal(err)
	}
	if contains(deletingEndpoint.Finalizers, lifetimeFinalizer) || len(deletingEndpoint.Finalizers) != 1 {
		t.Fatalf("manual deletion did not release Crossplane finalizer ordering: %v", deletingEndpoint.Finalizers)
	}
	gotTemplate := &serverlessv1alpha1.Template{}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(template), gotTemplate); err != nil {
		t.Fatal(err)
	}
	if gotTemplate.Annotations[templateReapAnnotation] != "" {
		t.Fatal("manual pre-expiry deletion unexpectedly scheduled Template cleanup")
	}
}

func TestSharedTemplateIsNotDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	template := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "runpod-system"},
		Status:     serverlessv1alpha1.TemplateStatus{AtProvider: serverlessv1alpha1.TemplateObservation{ID: "template_1"}},
	}
	expired := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name: "expired", Namespace: "runpod-system", CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
			DeletionTimestamp: func() *metav1.Time { value := metav1.Now(); return &value }(), Finalizers: []string{lifetimeFinalizer},
		},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{
			MaxLifetimeSeconds: 3600, TemplateIDRef: &xpv2.Reference{Name: template.Name},
		}},
	}
	other := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "still-using", Namespace: "runpod-system"},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{
			MaxLifetimeSeconds: 3600, TemplateIDRef: &xpv2.Reference{Name: template.Name},
		}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, expired, other).Build()
	r := &reconciler{kube: kube, reader: kube, now: time.Now}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(expired)}); err != nil {
		t.Fatal(err)
	}
	marked := &serverlessv1alpha1.Template{}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(template), marked); err != nil || marked.Annotations[templateReapAnnotation] == "" {
		t.Fatalf("shared Template was not marked for eventual cleanup: template=%+v err=%v", marked, err)
	}
	if _, err := (&templateReconciler{kube: kube, reader: kube}).Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(template)}); err != nil {
		t.Fatal(err)
	}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(template), &serverlessv1alpha1.Template{}); err != nil {
		t.Fatal("shared Template was deleted")
	}
}

func TestTemplateExpiresWithoutAnyEndpointObject(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	now := time.Now()
	template := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "runpod-system", CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour)), Finalizers: []string{"finalizer.managedresource.crossplane.io"}},
		Spec:       serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{MaxLifetimeSeconds: 3600}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	r := &templateReconciler{kube: kube, reader: kube, now: func() time.Time { return now }}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(template)}); err != nil {
		t.Fatal(err)
	}
	got := &serverlessv1alpha1.Template{}
	if err := kube.Get(context.Background(), client.ObjectKeyFromObject(template), got); err != nil {
		t.Fatal(err)
	}
	if got.DeletionTimestamp.IsZero() {
		t.Fatal("independent Template lifetime did not initiate deletion after permanent Endpoint absence")
	}
}

func TestExpiredTemplateWaitsForDirectEndpointReference(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	now := time.Now()
	template := &serverlessv1alpha1.Template{
		ObjectMeta: metav1.ObjectMeta{Name: "referenced", Namespace: "runpod-system", CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Hour))},
		Spec:       serverlessv1alpha1.TemplateSpec{ForProvider: serverlessv1alpha1.TemplateParameters{MaxLifetimeSeconds: 3600}},
	}
	endpoint := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "direct-only", Namespace: template.Namespace},
		Spec:       serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{TemplateIDRef: &xpv2.Reference{Name: template.Name}}},
	}
	staleCache := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template.DeepCopy(), endpoint).Build()
	r := &templateReconciler{kube: staleCache, reader: direct, now: func() time.Time { return now }}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(template)})
	if err != nil || result.RequeueAfter != cleanupPoll {
		t.Fatalf("reconcile=%+v err=%v", result, err)
	}
	got := &serverlessv1alpha1.Template{}
	if err := staleCache.Get(context.Background(), client.ObjectKeyFromObject(template), got); err != nil || !got.DeletionTimestamp.IsZero() {
		t.Fatalf("Template deleted despite direct Endpoint reference: template=%+v err=%v", got, err)
	}
}

func TestExpiredEndpointUsesDirectReaderBeforeDeletion(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = serverlessv1alpha1.SchemeBuilder.AddToScheme(scheme)
	_ = verificationv1alpha1.SchemeBuilder.AddToScheme(scheme)
	ep := &serverlessv1alpha1.Endpoint{
		ObjectMeta: metav1.ObjectMeta{
			Name: "endpoint", Namespace: "runpod-system",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
			Finalizers:        []string{lifetimeFinalizer},
		},
		Spec: serverlessv1alpha1.EndpointSpec{ForProvider: serverlessv1alpha1.EndpointParameters{MaxLifetimeSeconds: 3600}},
	}
	check := &verificationv1alpha1.EndpointCheck{
		ObjectMeta: metav1.ObjectMeta{Name: "direct-only", Namespace: ep.Namespace},
		Spec: verificationv1alpha1.EndpointCheckSpec{ForProvider: verificationv1alpha1.EndpointCheckParameters{
			EndpointIDRef: &xpv2.Reference{Name: ep.Name},
		}},
	}
	staleCache := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ep).Build()
	direct := fake.NewClientBuilder().WithScheme(scheme).WithObjects(check).Build()
	r := &reconciler{kube: staleCache, reader: direct, now: time.Now}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ep)})
	if err != nil || result.RequeueAfter != cleanupPoll {
		t.Fatalf("reconcile=%+v err=%v", result, err)
	}
	remaining := &serverlessv1alpha1.Endpoint{}
	if err := staleCache.Get(context.Background(), client.ObjectKeyFromObject(ep), remaining); err != nil || !remaining.DeletionTimestamp.IsZero() {
		t.Fatalf("direct EndpointCheck was missed before Endpoint deletion: endpoint=%+v err=%v", remaining, err)
	}
}
