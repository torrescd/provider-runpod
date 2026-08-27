// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package janitor enforces hard experiment lifetimes through Kubernetes
// deletion. Crossplane finalizers then perform idempotent external cleanup.
package janitor

import (
	"context"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
)

const cleanupPoll = 2 * time.Second

// lifetimeFinalizer keeps the Kubernetes Endpoint as a sequencing record until
// Crossplane has removed its external-resource finalizer. Only then may the
// janitor delete the referenced Template. This prevents RunPod template
// deletion from racing ahead of endpoint deletion.
const lifetimeFinalizer = "janitor.runpod.crossplane.io/lifetime"

func Setup(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("endpoint-lifetime-janitor.runpod.crossplane.io").
		For(&serverlessv1alpha1.Endpoint{}).
		Complete(&reconciler{kube: mgr.GetClient(), now: time.Now})
}

type reconciler struct {
	kube client.Client
	now  func() time.Time
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
	if ep.DeletionTimestamp.IsZero() {
		if remaining := expiresAt.Sub(r.now()); remaining > 0 {
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

	if ep.DeletionTimestamp.IsZero() {
		if err := r.kube.Delete(ctx, ep); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: cleanupPoll}, nil
	}

	if !contains(ep.Finalizers, lifetimeFinalizer) {
		// A deletion that raced initial finalizer installation is still safely
		// route-drained, but there is no janitor sequencing lock to release.
		return ctrl.Result{}, nil
	}
	if hasFinalizerOtherThan(ep.Finalizers, lifetimeFinalizer) {
		// Crossplane's managed-resource finalizer is removed only after the
		// external endpoint is gone (or was never created).
		return ctrl.Result{RequeueAfter: cleanupPoll}, nil
	}

	if !r.now().Before(expiresAt) && shouldDeleteTemplate(ep.Spec.ForProvider.DeleteReferencedTemplateOnExpiry) && ep.Spec.ForProvider.TemplateIDRef != nil {
		if err := r.deleteUnsharedTemplate(ctx, ep); err != nil {
			return ctrl.Result{}, err
		}
	}
	ep.Finalizers = remove(ep.Finalizers, lifetimeFinalizer)
	if err := r.kube.Update(ctx, ep); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *reconciler) deleteChecks(ctx context.Context, ep *serverlessv1alpha1.Endpoint) (bool, error) {
	checks := &verificationv1alpha1.EndpointCheckList{}
	if err := r.kube.List(ctx, checks, client.InNamespace(ep.Namespace)); err != nil {
		return false, err
	}
	found := false
	for i := range checks.Items {
		p := checks.Items[i].Spec.ForProvider
		externalID := meta.GetExternalName(ep)
		if externalID == "" {
			externalID = ep.Status.AtProvider.ID
		}
		if (p.EndpointIDRef != nil && p.EndpointIDRef.Name == ep.Name) ||
			(p.EndpointID != "" && p.EndpointID == externalID) {
			if err := r.kube.Delete(ctx, &checks.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				return false, err
			}
			found = true
		}
	}
	return found, nil
}

func (r *reconciler) deleteUnsharedTemplate(ctx context.Context, ep *serverlessv1alpha1.Endpoint) error {
	template := &serverlessv1alpha1.Template{}
	key := types.NamespacedName{Namespace: ep.Namespace, Name: ep.Spec.ForProvider.TemplateIDRef.Name}
	if err := r.kube.Get(ctx, key, template); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	endpoints := &serverlessv1alpha1.EndpointList{}
	if err := r.kube.List(ctx, endpoints, client.InNamespace(ep.Namespace)); err != nil {
		return err
	}
	for i := range endpoints.Items {
		other := &endpoints.Items[i]
		if (ep.UID != "" && other.UID == ep.UID) || (other.Name == ep.Name && other.Namespace == ep.Namespace) {
			continue
		}
		p := other.Spec.ForProvider
		if (p.TemplateIDRef != nil && p.TemplateIDRef.Name == template.Name) ||
			(p.TemplateID != "" && p.TemplateID == template.Status.AtProvider.ID) {
			return nil
		}
	}
	if err := r.kube.Delete(ctx, template); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func hasFinalizerOtherThan(values []string, own string) bool {
	for _, value := range values {
		if value != own {
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
