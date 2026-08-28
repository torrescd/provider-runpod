// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpointcheck

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	"github.com/torrescd/provider-runpod/internal/credentials"
	"github.com/torrescd/provider-runpod/internal/identifier"
	"github.com/torrescd/provider-runpod/internal/inference"
)

const pollInterval = 30 * time.Second

const defaultVerificationInterval = time.Hour

const endpointReconcileRequestAnnotation = "crossplane.io/reconcile-requested-at"

const workerObservationHandshake = 30 * time.Second

var errEndpointLifetimeExpired = errors.New("referenced Endpoint lifetime expired")

// RouterDrainFinalizer makes route withdrawal an acknowledged ordering gate:
// model-router removes it only after the deleting check is absent from routing.
const RouterDrainFinalizer = "router.runpod.crossplane.io/drain"

type verifier interface {
	CheckHealth(context.Context) error
	Verify(context.Context, string) (inference.Result, error)
}

var newVerifier = func(endpointID string, token []byte, timeout time.Duration) (verifier, error) {
	return inference.New(endpointID, token, timeout)
}

// Setup adds the EndpointCheck controller to model-router's namespace-scoped
// manager. Secret and Endpoint reads use the direct API reader, never its cache.
func Setup(mgr ctrl.Manager) error {
	r := &reconciler{kube: mgr.GetClient(), reader: mgr.GetAPIReader()}
	return ctrl.NewControllerManagedBy(mgr).
		Named("endpointcheck.verification.runpod.crossplane.io").
		For(&verificationv1alpha1.EndpointCheck{}).
		Complete(r)
}

type reconciler struct {
	kube   client.Client
	reader client.Reader
	now    func() time.Time
}

type endpointBinding struct {
	id                 string
	resourceUID        string
	resourceGeneration int64
	version            int32
	expiresAt          time.Time
	templateUID        string
	templateGeneration int64
	templateImage      string
	workerProven       bool
	workerObservedAt   time.Time
}

func (r *reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	check := &verificationv1alpha1.EndpointCheck{}
	// Read the check itself directly. A stale cached status event must not repeat
	// the billable admission probe before the cache observes our prior status
	// write. Mutations still go through the controller client.
	if err := r.reader.Get(ctx, req.NamespacedName, check); err != nil {
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
		return ctrl.Result{Requeue: true}, r.kube.Update(ctx, check)
	}
	now := r.currentTime()
	expiresAt := check.CreationTimestamp.Add(time.Duration(check.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if !now.Before(expiresAt) {
		// DeletionTimestamp immediately withdraws the route from model-router.
		return ctrl.Result{}, r.kube.Delete(ctx, check)
	}
	binding, err := r.resolveEndpoint(ctx, check)
	if errors.Is(err, errEndpointLifetimeExpired) {
		return ctrl.Result{}, r.kube.Delete(ctx, check)
	}
	if err != nil {
		return r.fail(ctx, check, inference.Result{}, err, now, expiresAt)
	}
	if binding.expiresAt.Before(expiresAt) {
		expiresAt = binding.expiresAt
	}

	// Status writes also enqueue this object. Avoid a self-triggered verification
	// loop while preserving the fixed 30-second metadata liveness cadence. The
	// referenced Endpoint deadline was direct-read before taking this shortcut.
	workerAdmissionPending := check.Status.ObservedGeneration == check.Generation &&
		check.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue &&
		check.Status.AtProvider.Healthy && check.Status.AtProvider.ModelVerified && check.Status.AtProvider.ToolCallVerified &&
		!check.Status.AtProvider.LastVerifiedAt.IsZero()
	if check.Status.ObservedGeneration == check.Generation && !check.Status.AtProvider.LastCheckedAt.IsZero() && !workerAdmissionPending {
		next := check.Status.AtProvider.LastCheckedAt.Add(pollInterval)
		if now.Before(next) {
			return ctrl.Result{RequeueAfter: minDuration(next.Sub(now), expiresAt.Sub(now))}, nil
		}
	}

	ref := check.Spec.ForProvider.InferenceCredentialsSecretRef
	secret := &corev1.Secret{}
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: check.Namespace, Name: ref.Name}, secret); err != nil {
		return r.fail(ctx, check, inference.Result{}, errors.New("cannot read inference credential Secret"), now, expiresAt)
	}
	if err := credentials.RequirePurpose(secret, credentials.PurposeInference); err != nil {
		return r.fail(ctx, check, inference.Result{}, err, now, expiresAt)
	}
	token, ok := secret.Data[ref.Key]
	if !ok {
		return r.fail(ctx, check, inference.Result{}, errors.New("inference credential Secret key is absent"), now, expiresAt)
	}
	timeout := time.Duration(check.Spec.ForProvider.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 3 * time.Minute
	}
	verificationDue := fullVerificationDue(check, binding, secret.ResourceVersion, now)
	if workerAdmissionPending && check.Status.AtProvider.CredentialsSecretResourceVersion == secret.ResourceVersion &&
		verificationAdmitted(check, binding, secret.ResourceVersion) {
		if binding.workerProven && binding.workerObservedAt.After(check.Status.AtProvider.LastVerificationAttemptAt.Time) {
			check.Status.AtProvider.LastCheckedAt = metav1.NewTime(now)
			check.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
			if err := r.kube.Status().Update(ctx, check); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: nextPoll(expiresAt, now)}, nil
		}
		if now.Sub(check.Status.AtProvider.LastVerificationAttemptAt.Time) <= workerObservationHandshake {
			if err := r.requestEndpointObservation(ctx, check, now); err != nil {
				return r.failLiveness(ctx, check, errors.New("cannot request post-verification worker observation"), now, expiresAt)
			}
			return ctrl.Result{RequeueAfter: minDuration(2*time.Second, expiresAt.Sub(now))}, nil
		}
		if !verificationDue {
			return ctrl.Result{RequeueAfter: nextPoll(expiresAt, now)}, nil
		}
		// The worker may have scaled to zero before the management controller
		// observed it. Once the explicit costly-verification interval elapses,
		// fall through to a new bounded wake/observe handshake instead of leaving
		// this check permanently unavailable.
	}
	if !verificationDue {
		v, err := newVerifier(binding.id, token, timeout)
		if err != nil {
			return r.fail(ctx, check, inference.Result{}, err, now, expiresAt)
		}
		if err := v.CheckHealth(ctx); err != nil {
			return r.failLiveness(ctx, check, errors.New("authenticated endpoint liveness check failed"), now, expiresAt)
		}
		check.Status.AtProvider.EndpointID = binding.id
		check.Status.AtProvider.EndpointResourceUID = binding.resourceUID
		check.Status.AtProvider.EndpointResourceGeneration = binding.resourceGeneration
		version := binding.version
		check.Status.AtProvider.EndpointVersion = &version
		check.Status.AtProvider.InferenceURL = inferenceURL(binding.id)
		check.Status.AtProvider.LastCheckedAt = metav1.NewTime(now)
		check.Status.ObservedGeneration = check.Generation
		if verificationAdmitted(check, binding, secret.ResourceVersion) {
			check.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
		} else {
			retryPending := errors.New("full verification retry interval has not elapsed")
			check.Status.SetConditions(xpv2.Unavailable().WithMessage(retryPending.Error()), xpv2.ReconcileError(retryPending))
		}
		if err := r.kube.Status().Update(ctx, check); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: nextPoll(expiresAt, now)}, nil
	}
	v, err := newVerifier(binding.id, token, timeout)
	if err != nil {
		return r.fail(ctx, check, inference.Result{}, err, now, expiresAt)
	}
	result, err := v.Verify(ctx, check.Spec.ForProvider.ExpectedModelID)
	if err != nil {
		return r.failVerification(ctx, check, result, err, binding, secret.ResourceVersion, now, expiresAt)
	}
	check.Status.AtProvider = observation(binding, result, secret.ResourceVersion, now, now, now)
	check.Status.ObservedGeneration = check.Generation
	if binding.workerProven {
		check.Status.SetConditions(xpv2.Available(), xpv2.ReconcileSuccess())
	} else {
		pending := errors.New("authenticated verification passed; awaiting a directly observed bounded active worker")
		check.Status.SetConditions(xpv2.Unavailable().WithMessage(pending.Error()), xpv2.ReconcileError(pending))
	}
	if err := r.kube.Status().Update(ctx, check); err != nil {
		return ctrl.Result{}, err
	}
	if !binding.workerProven {
		if err := r.requestEndpointObservation(ctx, check, now); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: minDuration(2*time.Second, expiresAt.Sub(now))}, nil
	}
	return ctrl.Result{RequeueAfter: nextPoll(expiresAt, now)}, nil
}

func (r *reconciler) requestEndpointObservation(ctx context.Context, check *verificationv1alpha1.EndpointCheck, now time.Time) error {
	ref := check.Spec.ForProvider.EndpointIDRef
	if ref == nil {
		return errors.New("endpointIdRef is required")
	}
	endpoint := &serverlessv1alpha1.Endpoint{}
	key := types.NamespacedName{Namespace: check.Namespace, Name: ref.Name}
	if err := r.reader.Get(ctx, key, endpoint); err != nil {
		return errors.New("cannot read Endpoint for worker observation request")
	}
	before := endpoint.DeepCopy()
	if endpoint.Annotations == nil {
		endpoint.Annotations = map[string]string{}
	}
	endpoint.Annotations[endpointReconcileRequestAnnotation] = now.UTC().Format(time.RFC3339Nano)
	if err := r.kube.Patch(ctx, endpoint, client.MergeFrom(before)); err != nil {
		return errors.New("cannot annotate Endpoint for worker observation")
	}
	return nil
}

func (r *reconciler) failLiveness(ctx context.Context, check *verificationv1alpha1.EndpointCheck, cause error, now, expiresAt time.Time) (ctrl.Result, error) {
	// Preserve the still-current model/tool verification so recovery from a
	// transient cheap health failure does not itself trigger another billable
	// completion. Ready remains false until a later health call succeeds.
	check.Status.AtProvider.LastCheckedAt = metav1.NewTime(now)
	check.Status.ObservedGeneration = check.Generation
	safeCause := errors.New(safeMessage(cause))
	check.Status.SetConditions(xpv2.Unavailable().WithMessage(safeCause.Error()), xpv2.ReconcileError(safeCause))
	if err := r.kube.Status().Update(ctx, check); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: nextPoll(expiresAt, now)}, nil
}

func (r *reconciler) resolveEndpoint(ctx context.Context, check *verificationv1alpha1.EndpointCheck) (endpointBinding, error) {
	p := check.Spec.ForProvider
	if p.EndpointIDRef == nil {
		return endpointBinding{}, errors.New("endpointIdRef is required; direct external endpoint IDs cannot prove the bounded Endpoint state")
	}
	ep := &serverlessv1alpha1.Endpoint{}
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: check.Namespace, Name: p.EndpointIDRef.Name}, ep); err != nil {
		return endpointBinding{}, errors.New("cannot resolve referenced Endpoint")
	}
	expiresAt := ep.CreationTimestamp.Add(time.Duration(ep.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if !r.currentTime().Before(expiresAt) {
		return endpointBinding{}, errEndpointLifetimeExpired
	}
	if !ep.DeletionTimestamp.IsZero() || ep.Status.ObservedGeneration != ep.Generation ||
		ep.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue ||
		ep.Status.GetCondition(xpv2.TypeSynced).Status != corev1.ConditionTrue ||
		!ep.Status.AtProvider.FlashBootEvidenceCurrent(r.currentTime()) {
		return endpointBinding{}, errors.New("referenced Endpoint is not Ready for its current generation")
	}
	if ep.Status.AtProvider.Version == nil {
		return endpointBinding{}, errors.New("referenced Endpoint has no observed rollout version")
	}
	endpointID := meta.GetExternalName(ep)
	if identifier.ValidateRunPodID(endpointID) != nil || endpointID != ep.Status.AtProvider.ID {
		return endpointBinding{}, errors.New("referenced Endpoint external-name is not bound to its controller-observed ID")
	}
	templateRef := ep.Spec.ForProvider.TemplateIDRef
	if templateRef == nil {
		return endpointBinding{}, errors.New("referenced Endpoint has no managed Template reference")
	}
	template := &serverlessv1alpha1.Template{}
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: ep.Namespace, Name: templateRef.Name}, template); err != nil {
		return endpointBinding{}, errors.New("cannot resolve Endpoint Template revision")
	}
	if !template.DeletionTimestamp.IsZero() || template.Status.ObservedGeneration != template.Generation ||
		template.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue ||
		template.Status.GetCondition(xpv2.TypeSynced).Status != corev1.ConditionTrue ||
		string(template.UID) != ep.Status.AtProvider.TemplateResourceUID ||
		template.Generation != ep.Status.AtProvider.TemplateResourceGeneration ||
		template.Spec.ForProvider.ImageName != ep.Status.AtProvider.TemplateImageDigest {
		return endpointBinding{}, errors.New("referenced Endpoint is not bound to the current managed Template revision")
	}
	templateID := meta.GetExternalName(template)
	if identifier.ValidateRunPodID(templateID) != nil || templateID != template.Status.AtProvider.ID ||
		ep.Status.AtProvider.TemplateID != templateID {
		return endpointBinding{}, errors.New("referenced Endpoint and Template external IDs are not controller-observed bindings")
	}
	templateExpiresAt := template.CreationTimestamp.Add(time.Duration(template.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if !template.CreationTimestamp.IsZero() && !r.currentTime().Before(templateExpiresAt) {
		return endpointBinding{}, errEndpointLifetimeExpired
	}
	return endpointBinding{
		id:                 endpointID,
		resourceUID:        string(ep.UID),
		resourceGeneration: ep.Generation,
		version:            *ep.Status.AtProvider.Version,
		expiresAt:          expiresAt,
		templateUID:        string(template.UID),
		templateGeneration: template.Generation,
		templateImage:      template.Spec.ForProvider.ImageName,
		workerProven:       ep.Status.AtProvider.WorkerSecurityEvidenceCurrent(),
		workerObservedAt:   ep.Status.AtProvider.WorkerSecurityObservedAt.Time,
	}, nil
}

func (r *reconciler) fail(ctx context.Context, check *verificationv1alpha1.EndpointCheck, result inference.Result, cause error, now, expiresAt time.Time) (ctrl.Result, error) {
	check.Status.AtProvider = observation(endpointBinding{}, result, "", now, time.Time{}, time.Time{})
	check.Status.ObservedGeneration = check.Generation
	safeCause := errors.New(safeMessage(cause))
	check.Status.SetConditions(xpv2.Unavailable().WithMessage(safeCause.Error()), xpv2.ReconcileError(safeCause))
	if err := r.kube.Status().Update(ctx, check); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: nextPoll(expiresAt, now)}, nil
}

func (r *reconciler) failVerification(ctx context.Context, check *verificationv1alpha1.EndpointCheck, result inference.Result, cause error, binding endpointBinding, secretResourceVersion string, now, expiresAt time.Time) (ctrl.Result, error) {
	// Record the failed or timed-out costly attempt against the exact endpoint,
	// generation, and credential version. Subsequent 30-second reconciles only
	// perform the cheap health probe until the configured retry interval elapses.
	check.Status.AtProvider = observation(binding, result, secretResourceVersion, now, now, time.Time{})
	check.Status.ObservedGeneration = check.Generation
	safeCause := errors.New(safeMessage(cause))
	check.Status.SetConditions(xpv2.Unavailable().WithMessage(safeCause.Error()), xpv2.ReconcileError(safeCause))
	if err := r.kube.Status().Update(ctx, check); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: nextPoll(expiresAt, now)}, nil
}

func nextPoll(expiresAt, now time.Time) time.Duration {
	remaining := expiresAt.Sub(now)
	if remaining <= 0 {
		return time.Second
	}
	if remaining < pollInterval {
		return remaining
	}
	return pollInterval
}

func observation(binding endpointBinding, result inference.Result, secretResourceVersion string, checkedAt, attemptedAt, verifiedAt time.Time) verificationv1alpha1.EndpointCheckObservation {
	version := binding.version
	var attempted metav1.Time
	if !attemptedAt.IsZero() {
		attempted = metav1.NewTime(attemptedAt)
	}
	var verified metav1.Time
	if !verifiedAt.IsZero() {
		verified = metav1.NewTime(verifiedAt)
	}
	return verificationv1alpha1.EndpointCheckObservation{
		EndpointID: binding.id, EndpointResourceUID: binding.resourceUID,
		EndpointResourceGeneration: binding.resourceGeneration, EndpointVersion: &version,
		TemplateResourceUID: binding.templateUID, TemplateResourceGeneration: binding.templateGeneration, TemplateImageDigest: binding.templateImage,
		Healthy: result.Healthy, ModelVerified: result.ModelVerified,
		ToolCallVerified: result.ToolCallVerified, InferenceURL: inferenceURL(binding.id),
		CredentialsSecretResourceVersion: secretResourceVersion,
		LastCheckedAt:                    metav1.NewTime(checkedAt),
		LastVerificationAttemptAt:        attempted,
		LastVerifiedAt:                   verified,
	}
}

func fullVerificationDue(check *verificationv1alpha1.EndpointCheck, binding endpointBinding, secretResourceVersion string, now time.Time) bool {
	observed := check.Status.AtProvider
	if check.Status.ObservedGeneration != check.Generation ||
		observed.EndpointID != binding.id || observed.EndpointResourceUID != binding.resourceUID ||
		observed.EndpointResourceGeneration != binding.resourceGeneration || observed.EndpointVersion == nil || *observed.EndpointVersion != binding.version ||
		observed.TemplateResourceUID != binding.templateUID || observed.TemplateResourceGeneration != binding.templateGeneration ||
		observed.TemplateImageDigest != binding.templateImage ||
		observed.CredentialsSecretResourceVersion != secretResourceVersion {
		return true
	}
	interval := time.Duration(check.Spec.ForProvider.VerificationIntervalSeconds) * time.Second
	if interval == 0 {
		interval = defaultVerificationInterval
	}
	if observed.LastVerificationAttemptAt.IsZero() {
		return true
	}
	age := now.Sub(observed.LastVerificationAttemptAt.Time)
	return age < 0 || age >= interval
}

func verificationAdmitted(check *verificationv1alpha1.EndpointCheck, binding endpointBinding, secretResourceVersion string) bool {
	observed := check.Status.AtProvider
	return check.Status.ObservedGeneration == check.Generation &&
		observed.EndpointID == binding.id && observed.EndpointResourceUID == binding.resourceUID &&
		observed.EndpointResourceGeneration == binding.resourceGeneration && observed.EndpointVersion != nil && *observed.EndpointVersion == binding.version &&
		observed.TemplateResourceUID == binding.templateUID && observed.TemplateResourceGeneration == binding.templateGeneration &&
		observed.TemplateImageDigest == binding.templateImage &&
		observed.CredentialsSecretResourceVersion == secretResourceVersion &&
		observed.Healthy && observed.ModelVerified && observed.ToolCallVerified && !observed.LastVerifiedAt.IsZero() &&
		binding.workerProven && !observed.LastVerificationAttemptAt.IsZero() &&
		binding.workerObservedAt.After(observed.LastVerificationAttemptAt.Time)
}

func (r *reconciler) currentTime() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func inferenceURL(endpointID string) string {
	if endpointID == "" {
		return ""
	}
	return "https://api.runpod.ai/v2/" + endpointID + "/openai/v1"
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
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
