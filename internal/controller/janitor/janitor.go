// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package janitor enforces hard experiment lifetimes through Kubernetes
// deletion. Crossplane finalizers then perform idempotent external cleanup.
package janitor

import (
	"context"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	xperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
)

const cleanupPoll = 2 * time.Second

const (
	// lifetimeFinalizer keeps the Kubernetes Endpoint as a route-drain ordering
	// record. It must be removed before Crossplane can run ExternalDelete:
	// crossplane-runtime v2 deliberately refuses deletion while more than its
	// own managed-resource finalizer is present.
	lifetimeFinalizer = "janitor.runpod.crossplane.io/lifetime"
	// templateReapAnnotation transfers cleanup intent to an independent object
	// before the Endpoint sequencing record is released.
	templateReapAnnotation = "janitor.runpod.crossplane.io/delete-when-unreferenced"
)

var setupController = Setup

// SetupGated defers the janitor until every CRD it watches, lists, or deletes
// is established. This preserves the provider package's safe-start contract.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupController(mgr); err != nil {
			panic(xperrors.Wrap(err, "cannot setup Endpoint lifetime janitor"))
		}
	}, serverlessv1alpha1.EndpointGroupVersionKind,
		serverlessv1alpha1.TemplateGroupVersionKind,
		verificationv1alpha1.EndpointCheckGroupVersionKind)
	return nil
}

func Setup(mgr ctrl.Manager) error {
	if err := ctrl.NewControllerManagedBy(mgr).
		Named("endpoint-lifetime-janitor.runpod.crossplane.io").
		For(&serverlessv1alpha1.Endpoint{}).
		Complete(&reconciler{kube: mgr.GetClient(), reader: mgr.GetAPIReader(), now: time.Now}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("template-expiry-reaper.runpod.crossplane.io").
		For(&serverlessv1alpha1.Template{}).
		Complete(&templateReconciler{kube: mgr.GetClient(), reader: mgr.GetAPIReader(), now: time.Now})
}

type reconciler struct {
	kube   client.Client
	reader client.Reader
	now    func() time.Time
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	ep := &serverlessv1alpha1.Endpoint{}
	if err := r.kube.Get(ctx, req.NamespacedName, ep); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if ep.DeletionTimestamp.IsZero() && !contains(ep.Finalizers, lifetimeFinalizer) {
		ep.Finalizers = append(ep.Finalizers, lifetimeFinalizer)
		return ctrl.Result{}, r.kube.Update(ctx, ep)
	}
	expiresAt := ep.CreationTimestamp.Add(time.Duration(ep.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	now := r.now()
	expired := !now.Before(expiresAt)
	if ep.DeletionTimestamp.IsZero() {
		if remaining := expiresAt.Sub(now); remaining > 0 {
			return ctrl.Result{RequeueAfter: remaining}, nil
		}
	}

	// Whether deletion was initiated by TTL or by a user, stop routing first.
	// The Endpoint managed reconciler independently refuses external deletion
	// while any matching EndpointCheck exists.
	checksRemain, err := r.deleteChecks(ctx, ep)
	if err != nil {
		return ctrl.Result{}, err
	}
	if checksRemain {
		return ctrl.Result{RequeueAfter: cleanupPoll}, nil
	}
	if expired && shouldDeleteTemplate(ep.Spec.ForProvider.DeleteReferencedTemplateOnExpiry) && ep.Spec.ForProvider.TemplateIDRef != nil {
		if err := r.markTemplateForReaping(ctx, ep); err != nil {
			return ctrl.Result{}, err
		}
	}

	if ep.DeletionTimestamp.IsZero() {
		if err := r.kube.Delete(ctx, ep); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: cleanupPoll}, nil
	}

	if contains(ep.Finalizers, lifetimeFinalizer) {
		// Release our finalizer while Crossplane's finalizer is still present.
		// This is what permits the managed reconciler to call ExternalDelete.
		ep.Finalizers = remove(ep.Finalizers, lifetimeFinalizer)
		if err := r.kube.Update(ctx, ep); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *reconciler) deleteChecks(ctx context.Context, ep *serverlessv1alpha1.Endpoint) (bool, error) {
	checks := &verificationv1alpha1.EndpointCheckList{}
	if err := r.reader.List(ctx, checks, client.InNamespace(ep.Namespace)); err != nil {
		return false, err
	}
	found := false
	for i := range checks.Items {
		p := checks.Items[i].Spec.ForProvider
		if p.EndpointIDRef != nil && p.EndpointIDRef.Name == ep.Name {
			if err := r.kube.Delete(ctx, &checks.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
			found = true
		}
	}
	return found, nil
}

func (r *reconciler) markTemplateForReaping(ctx context.Context, ep *serverlessv1alpha1.Endpoint) error {
	template := &serverlessv1alpha1.Template{}
	key := types.NamespacedName{Namespace: ep.Namespace, Name: ep.Spec.ForProvider.TemplateIDRef.Name}
	if err := r.reader.Get(ctx, key, template); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	if _, exists := template.Annotations[templateReapAnnotation]; exists {
		return nil
	}
	marker := string(ep.UID)
	if marker == "" {
		marker = ep.Namespace + "/" + ep.Name
	}
	template.Annotations[templateReapAnnotation] = marker
	return r.kube.Update(ctx, template)
}

type templateReconciler struct {
	kube   client.Client
	reader client.Reader
	now    func() time.Time
}

func (r *templateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	template := &serverlessv1alpha1.Template{}
	if err := r.reader.Get(ctx, req.NamespacedName, template); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !template.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	reapRequested := template.Annotations[templateReapAnnotation] != ""
	if template.Spec.ForProvider.MaxLifetimeSeconds > 0 && !template.CreationTimestamp.IsZero() {
		now := time.Now()
		if r.now != nil {
			now = r.now()
		}
		expiresAt := template.CreationTimestamp.Add(time.Duration(template.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
		if now.Before(expiresAt) && !reapRequested {
			return ctrl.Result{RequeueAfter: expiresAt.Sub(now)}, nil
		}
		if !now.Before(expiresAt) {
			reapRequested = true
		}
	}
	if !reapRequested {
		return ctrl.Result{}, nil
	}
	endpoints := &serverlessv1alpha1.EndpointList{}
	if err := r.reader.List(ctx, endpoints, client.InNamespace(template.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	for i := range endpoints.Items {
		p := endpoints.Items[i].Spec.ForProvider
		if p.TemplateIDRef != nil && p.TemplateIDRef.Name == template.Name {
			return ctrl.Result{RequeueAfter: cleanupPoll}, nil
		}
	}
	if err := r.kube.Delete(ctx, template); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func remove(values []string, target string) []string {
	for i, value := range values {
		if value == target {
			return append(values[:i], values[i+1:]...)
		}
	}
	return values
}

func shouldDeleteTemplate(v *bool) bool { return v == nil || *v }
