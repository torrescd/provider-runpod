// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

// Package janitor enforces hard experiment lifetimes through Kubernetes
// deletion. Crossplane finalizers then perform idempotent external cleanup.
package janitor

import (
	"context"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
)

const cleanupPoll = 2 * time.Second

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
	if !ep.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	expiresAt := ep.CreationTimestamp.Add(time.Duration(ep.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if remaining := expiresAt.Sub(r.now()); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}

	checks := &verificationv1alpha1.EndpointCheckList{}
	if err := r.kube.List(ctx, checks, client.InNamespace(ep.Namespace)); err != nil {
		return ctrl.Result{}, err
	}
	deletedCheck := false
	for i := range checks.Items {
		p := checks.Items[i].Spec.ForProvider
		if (p.EndpointIDRef != nil && p.EndpointIDRef.Name == ep.Name) ||
			(p.EndpointID != "" && p.EndpointID == ep.Status.AtProvider.ID) {
			if err := r.kube.Delete(ctx, &checks.Items[i]); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			deletedCheck = true
		}
	}
	if deletedCheck {
		return ctrl.Result{RequeueAfter: cleanupPoll}, nil
	}

	if shouldDeleteTemplate(ep.Spec.ForProvider.DeleteReferencedTemplateOnExpiry) && ep.Spec.ForProvider.TemplateIDRef != nil {
		t := &serverlessv1alpha1.Template{}
		key := types.NamespacedName{Namespace: ep.Namespace, Name: ep.Spec.ForProvider.TemplateIDRef.Name}
		if err := r.kube.Get(ctx, key, t); err == nil {
			if err := r.kube.Delete(ctx, t); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		} else if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	}
	if err := r.kube.Delete(ctx, ep); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func shouldDeleteTemplate(v *bool) bool { return v == nil || *v }
