// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpointcheck

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	xperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/inference"
)

const pollInterval = 30 * time.Second

// RouterDrainFinalizer makes route withdrawal an acknowledged ordering gate:
// model-router removes it only after the deleting check is absent from routing.
const RouterDrainFinalizer = "router.runpod.crossplane.io/drain"

type verifier interface {
	Verify(context.Context, string) (inference.Result, error)
}

var newVerifier = func(endpointID string, token []byte, timeout time.Duration) (verifier, error) {
	return inference.New(endpointID, token, timeout)
}

func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr); err != nil {
			panic(xperrors.Wrap(err, "cannot setup EndpointCheck controller"))
		}
	}, verificationv1alpha1.EndpointCheckGroupVersionKind)
	return nil
}

func Setup(mgr ctrl.Manager) error {
	r := &reconciler{kube: mgr.GetClient()}
	return ctrl.NewControllerManagedBy(mgr).
		Named("endpointcheck.verification.runpod.crossplane.io").
		For(&verificationv1alpha1.EndpointCheck{}).
		Complete(r)
}

type reconciler struct{ kube client.Client }

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	check := &verificationv1alpha1.EndpointCheck{}
	if err := r.kube.Get(ctx, req.NamespacedName, check); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !check.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if !containsString(check.Finalizers, RouterDrainFinalizer) {
		check.Finalizers = append(check.Finalizers, RouterDrainFinalizer)
		return ctrl.Result{}, r.kube.Update(ctx, check)
	}
	expiresAt := check.CreationTimestamp.Add(time.Duration(check.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if !time.Now().Before(expiresAt) {
		// DeletionTimestamp immediately withdraws the route from model-router.
		return ctrl.Result{}, r.kube.Delete(ctx, check)
	}

	endpointID, err := r.resolveEndpoint(ctx, check)
	if err != nil {
		return r.fail(ctx, check, inference.Result{}, err)
	}
	ref := check.Spec.ForProvider.InferenceCredentialsSecretRef
	if err := r.rejectManagementSecret(ctx, check.Namespace, ref.Name); err != nil {
		return r.fail(ctx, check, inference.Result{}, err)
	}
	secret := &corev1.Secret{}
	if err := r.kube.Get(ctx, types.NamespacedName{Namespace: check.Namespace, Name: ref.Name}, secret); err != nil {
		return r.fail(ctx, check, inference.Result{}, errors.New("cannot read inference credential Secret"))
	}
	token, ok := secret.Data[ref.Key]
	if !ok {
		return r.fail(ctx, check, inference.Result{}, errors.New("inference credential Secret key is absent"))
	}
	timeout := time.Duration(check.Spec.ForProvider.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 15 * time.Second
	}
	v, err := newVerifier(endpointID, token, timeout)
	if err != nil {
		return r.fail(ctx, check, inference.Result{}, err)
	}
	result, err := v.Verify(ctx, check.Spec.ForProvider.ExpectedModelID)
	if err != nil {
		return r.fail(ctx, check, result, err)
	}
	check.Status.AtProvider = observation(endpointID, result)
	check.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	if err := r.kube.Status().Update(ctx, check); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: nextPoll(expiresAt)}, nil
}

func (r *reconciler) resolveEndpoint(ctx context.Context, check *verificationv1alpha1.EndpointCheck) (string, error) {
	p := check.Spec.ForProvider
	if p.EndpointIDRef == nil {
		if p.EndpointID == "" {
			return "", errors.New("one of endpointId or endpointIdRef is required")
		}
		return p.EndpointID, nil
	}
	if p.EndpointID != "" {
		return "", errors.New("endpointId and endpointIdRef are mutually exclusive")
	}
	ep := &serverlessv1alpha1.Endpoint{}
	if err := r.kube.Get(ctx, types.NamespacedName{Namespace: check.Namespace, Name: p.EndpointIDRef.Name}, ep); err != nil {
		return "", errors.New("cannot resolve referenced Endpoint")
	}
	if !ep.DeletionTimestamp.IsZero() || ep.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue {
		return "", errors.New("referenced Endpoint is not Ready")
	}
	id := meta.GetExternalName(ep)
	if id == "" {
		return "", errors.New("referenced Endpoint has no external ID")
	}
	return id, nil
}

func (r *reconciler) rejectManagementSecret(ctx context.Context, namespace, name string) error {
	pcs := &apisv1alpha1.ProviderConfigList{}
	if err := r.kube.List(ctx, pcs, client.InNamespace(namespace)); err != nil {
		return errors.New("cannot enforce inference/management credential separation")
	}
	for i := range pcs.Items {
		ref := pcs.Items[i].Spec.Credentials.SecretRef
		if ref != nil && ref.Name == name && ref.Namespace == namespace {
			return errors.New("inference credential Secret must not be a management credential Secret")
		}
	}
	cpcs := &apisv1alpha1.ClusterProviderConfigList{}
	if err := r.kube.List(ctx, cpcs); err != nil {
		return errors.New("cannot enforce inference/management credential separation")
	}
	for i := range cpcs.Items {
		ref := cpcs.Items[i].Spec.Credentials.SecretRef
		if ref != nil && ref.Name == name && ref.Namespace == namespace {
			return errors.New("inference credential Secret must not be a management credential Secret")
		}
	}
	return nil
}

func (r *reconciler) fail(ctx context.Context, check *verificationv1alpha1.EndpointCheck, result inference.Result, cause error) (ctrl.Result, error) {
	check.Status.AtProvider = observation(check.Spec.ForProvider.EndpointID, result)
	check.Status.SetConditions(xpv2.Unavailable().WithMessage(safeMessage(cause)), xpv2.ReconcileError(cause))
	if err := r.kube.Status().Update(ctx, check); err != nil {
		return ctrl.Result{}, err
	}
	expiresAt := check.CreationTimestamp.Add(time.Duration(check.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	return ctrl.Result{RequeueAfter: nextPoll(expiresAt)}, nil
}

func nextPoll(expiresAt time.Time) time.Duration {
	remaining := time.Until(expiresAt)
	if remaining <= 0 {
		return time.Second
	}
	if remaining < pollInterval {
		return remaining
	}
	return pollInterval
}

func observation(endpointID string, result inference.Result) verificationv1alpha1.EndpointCheckObservation {
	url := ""
	if endpointID != "" {
		url = "https://api.runpod.ai/v2/" + endpointID + "/openai/v1"
	}
	return verificationv1alpha1.EndpointCheckObservation{
		EndpointID: endpointID, Healthy: result.Healthy, ModelVerified: result.ModelVerified,
		ToolCallVerified: result.ToolCallVerified, InferenceURL: url,
		LastCheckedAt: metav1.Now(),
	}
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func safeMessage(err error) string {
	msg := err.Error()
	if len(msg) > 256 {
		msg = msg[:256]
	}
	// Defense in depth. Clients never include tokens, but avoid retaining a
	// recognizable key prefix if a future dependency changes its errors.
	if i := strings.Index(msg, "rpa_"); i >= 0 {
		msg = msg[:i] + "[REDACTED]"
	}
	return msg
}
