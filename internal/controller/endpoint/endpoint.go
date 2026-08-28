// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpoint

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	xperrors "github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/statemetrics"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
	"github.com/torrescd/provider-runpod/internal/controller/management"
	"github.com/torrescd/provider-runpod/internal/identifier"
)

type service interface {
	GetEndpoint(context.Context, string) (*clientrunpod.Endpoint, error)
	FindEndpointByName(context.Context, string) (*clientrunpod.Endpoint, error)
	CreateEndpoint(context.Context, clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error)
	UpdateEndpoint(context.Context, string, clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error)
	DeleteEndpoint(context.Context, string) error
}

const (
	ambiguousCreateNameAnnotation       = "endpoint.serverless.runpod.crossplane.io/ambiguous-create-name"
	externalIDBoundAnnotation           = "endpoint.serverless.runpod.crossplane.io/external-id-bound"
	ambiguousAbsenceConfirmedAnnotation = "runpod.crossplane.io/ambiguous-create-absence-confirmed"
	externalCreatePendingAnnotation     = "crossplane.io/external-create-pending"
	defaultContainerDiskInGB            = int32(50)
)

var safeEndpointName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(xperrors.Wrap(err, "cannot setup Endpoint controller"))
		}
	}, serverlessv1alpha1.EndpointGroupVersionKind,
		serverlessv1alpha1.TemplateGroupVersionKind,
		verificationv1alpha1.EndpointCheckGroupVersionKind)
	return nil
}

func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(serverlessv1alpha1.EndpointGroupKind)
	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*serverlessv1alpha1.Endpoint](&connector{
			kube:   mgr.GetClient(),
			reader: mgr.GetAPIReader(),
			usage:  resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))), //nolint:staticcheck
		managed.WithInitializers(),
		managed.WithDeterministicExternalName(true),
	}
	if o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		opts = append(opts, managed.WithManagementPolicies())
	}
	if o.Features.Enabled(feature.EnableAlphaChangeLogs) {
		opts = append(opts, managed.WithChangeLogger(o.ChangeLogOptions.ChangeLogger))
	}
	if o.MetricOptions != nil {
		opts = append(opts, managed.WithMetricRecorder(o.MetricOptions.MRMetrics))
		if o.MetricOptions.MRStateMetrics != nil {
			recorder := statemetrics.NewMRStateRecorder(mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &serverlessv1alpha1.EndpointList{}, o.MetricOptions.PollStateMetricInterval)
			if err := mgr.Add(recorder); err != nil {
				return err
			}
		}
	}
	r := managed.NewReconciler(mgr, resource.ManagedKind(serverlessv1alpha1.EndpointGroupVersionKind), opts...)
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		For(&serverlessv1alpha1.Endpoint{}, builder.WithPredicates(resource.DesiredStateChanged())).
		Watches(&serverlessv1alpha1.Template{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, object client.Object) []reconcile.Request {
			return requestsForTemplate(ctx, mgr.GetClient(), object)
		}), builder.WithPredicates(predicate.ResourceVersionChangedPredicate{})).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

func requestsForTemplate(ctx context.Context, kube client.Reader, object client.Object) []reconcile.Request {
	endpoints := &serverlessv1alpha1.EndpointList{}
	if err := kube.List(ctx, endpoints, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range endpoints.Items {
		ref := endpoints.Items[i].Spec.ForProvider.TemplateIDRef
		if ref != nil && ref.Name == object.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: endpoints.Items[i].Namespace, Name: endpoints.Items[i].Name}})
		}
	}
	return requests
}

type templateBinding struct {
	id              string
	uid             string
	generation      int64
	image           string
	name            string
	containerDiskGB int32
	entrypoint      []string
	startCmd        []string
	ports           []string
}

type connector struct {
	kube   client.Client
	reader client.Reader
	usage  *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.TypedExternalClient[*serverlessv1alpha1.Endpoint], error) {
	svc, err := management.Connect(ctx, c.reader, c.usage, cr)
	if err != nil {
		return nil, err
	}
	template, err := resolveTemplate(ctx, c.reader, cr)
	if err != nil {
		return nil, err
	}
	return &external{service: svc, kube: c.kube, reader: c.reader, template: template}, nil
}

func (c *connector) resolveTemplateID(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (string, error) {
	return resolveTemplateID(ctx, c.reader, cr)
}

type external struct {
	service  service
	kube     client.Client
	reader   client.Reader
	template templateBinding
}

func (e *external) Observe(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalObservation, error) {
	if cr.DeletionTimestamp.IsZero() && cr.Annotations[ambiguousAbsenceConfirmedAnnotation] != "" {
		return managed.ExternalObservation{}, errors.New("ambiguous Endpoint absence is acknowledged; delete the CR to finalize cleanup; recreation is disabled")
	}
	pendingName := cr.Annotations[ambiguousCreateNameAnnotation]
	if meta.ExternalCreateIncomplete(cr) {
		pendingName = effectiveName(cr)
	}
	if pendingName != "" {
		if pendingName != effectiveName(cr) {
			return managed.ExternalObservation{}, errors.New("Endpoint recovery marker is not the controller-owned UID-scoped name")
		}
		got, err := e.service.FindEndpointByName(ctx, pendingName)
		if err != nil {
			if !cr.DeletionTimestamp.IsZero() && errors.Is(err, clientrunpod.ErrNotFound) {
				if ambiguousAbsenceAcknowledged(cr) {
					return managed.ExternalObservation{ResourceExists: false}, nil
				}
				return managed.ExternalObservation{}, errors.New("ambiguous Endpoint create absence is unproven; an exact pending-token operator acknowledgement is required before deletion can complete")
			}
			return managed.ExternalObservation{}, errors.New("ambiguous Endpoint create recovery is pending; no new create is permitted")
		}
		if got.Name != pendingName || identifier.ValidateRunPodID(got.ID) != nil {
			return managed.ExternalObservation{}, errors.New("Endpoint recovery returned an invalid external identity")
		}
		if !cr.DeletionTimestamp.IsZero() {
			expectedTemplateID, err := e.expectedTemplateIDForCleanup(ctx, cr)
			if err != nil {
				return managed.ExternalObservation{}, err
			}
			if expectedTemplateID != "" && got.TemplateID != expectedTemplateID {
				return managed.ExternalObservation{}, errors.New("Endpoint cleanup recovery did not match the referenced Template ID")
			}
			bindExternalID(cr, got.ID)
			cr.Status.AtProvider.ID = got.ID
			cr.Status.AtProvider.TemplateID = got.TemplateID
			return managed.ExternalObservation{ResourceExists: true, ResourceLateInitialized: true}, nil
		}
		desired := endpointInput(cr, e.template.id)
		desired.Name = pendingName
		if !endpointMatchesInput(desired, got) || !endpointTemplateMatches(got, e.template) || !endpointWorkersMatch(desired, got, e.template, cr.Spec.ForProvider.MaxWorkerCostMilliUSDPerHour) {
			return managed.ExternalObservation{}, clientrunpod.ErrAmbiguous
		}
		// A prior POST was ambiguous and RunPod's GET response does not expose
		// FlashBoot. Reassert the complete bounded input with an idempotent PATCH
		// before adopting the result, so readiness never rests on a guessed
		// retention setting.
		got, err = e.service.UpdateEndpoint(ctx, got.ID, desired)
		if err != nil || !endpointMatchesInput(desired, got) || !endpointTemplateMatches(got, e.template) || !endpointWorkersMatch(desired, got, e.template, cr.Spec.ForProvider.MaxWorkerCostMilliUSDPerHour) {
			return managed.ExternalObservation{}, errors.New("ambiguous Endpoint create recovery could not reassert bounded settings")
		}
		bindExternalID(cr, got.ID)
		setObservation(cr, got, true, e.template, len(got.Workers) > 0)
		cr.Status.ObservedGeneration = cr.Generation
		cr.Status.SetConditions(xpv2.Available())
		return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: true, ResourceLateInitialized: true}, nil
	}
	id := meta.GetExternalName(cr)
	if id == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if cr.Annotations[externalIDBoundAnnotation] != "true" {
		return managed.ExternalObservation{}, errors.New("v0.1 does not adopt an unverified external-name; only provider create or UID-scoped recovery may bind an ID")
	}
	if identifier.ValidateRunPodID(id) != nil {
		return managed.ExternalObservation{}, errors.New("Endpoint external-name is invalid")
	}
	statusBound := cr.Status.AtProvider.ID != ""
	if statusBound && cr.Status.AtProvider.ID != id {
		return managed.ExternalObservation{}, errors.New("Endpoint external-name differs from its persisted controller-observed ID")
	}
	got, err := e.service.GetEndpoint(ctx, id)
	if errors.Is(err, clientrunpod.ErrNotFound) {
		if !cr.DeletionTimestamp.IsZero() {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.New("bound RunPod Endpoint is missing; refusing to create a replacement under an immutable external-name")
	}
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if got == nil {
		return managed.ExternalObservation{}, errors.New("RunPod Endpoint GET returned no object")
	}
	// crossplane-runtime's critical-annotation update may discard status written
	// by ExternalCreate. The protected marker and immutable external-name are
	// therefore allowed to repopulate status only after an exact UID-scoped name
	// GET. A persisted status ID remains authoritative across external renames.
	if !statusBound {
		if got.Name != effectiveName(cr) {
			return managed.ExternalObservation{}, errors.New("Endpoint binding marker did not resolve to the controller-owned UID-scoped name")
		}
		cr.Status.AtProvider.ID = id
	}
	desired := endpointInput(cr, e.template.id)
	if err := unrepairableExternalState(got, desired); err != nil {
		cr.Status.ObservedGeneration = 0
		return managed.ExternalObservation{}, err
	}
	if !endpointTemplateMatches(got, e.template) {
		cr.Status.ObservedGeneration = 0
		return managed.ExternalObservation{}, errors.New("RunPod Endpoint omitted or mismatched its contemporaneous managed Template proof")
	}
	if !endpointWorkersMatch(desired, got, e.template, cr.Spec.ForProvider.MaxWorkerCostMilliUSDPerHour) {
		cr.Status.ObservedGeneration = 0
		return managed.ExternalObservation{}, errors.New("RunPod Endpoint active worker proof violated the bounded image, storage, placement, or Secure Cloud contract")
	}
	flashBootProven, templateRevisionProven := setObservation(cr, got, false, e.template, len(got.Workers) > 0)
	upToDate := endpointMatchesInput(desired, got) && flashBootProven && templateRevisionProven
	if upToDate {
		cr.Status.ObservedGeneration = cr.Generation
		cr.Status.SetConditions(xpv2.Available())
	} else {
		cr.Status.ObservedGeneration = 0
	}
	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: upToDate}, nil
}

func (e *external) Create(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv2.Creating())
	if cr.Annotations[ambiguousAbsenceConfirmedAnnotation] != "" {
		return managed.ExternalCreation{}, errors.New("ambiguous Endpoint absence acknowledgement forbids external creation; delete the CR to finalize")
	}
	if cr.UID == "" {
		return managed.ExternalCreation{}, errors.New("Kubernetes UID is required before creating a RunPod Endpoint")
	}
	if err := validate(cr, e.template.id); err != nil {
		return managed.ExternalCreation{}, err
	}
	desired := endpointInput(cr, e.template.id)
	if pendingName := cr.Annotations[ambiguousCreateNameAnnotation]; pendingName != "" {
		e.recoverAmbiguousCreate(ctx, cr, desired, pendingName)
		return managed.ExternalCreation{}, nil
	}
	if err := ensureLifetimeActive(cr, time.Now()); err != nil {
		return managed.ExternalCreation{}, err
	}
	got, err := e.service.CreateEndpoint(ctx, desired)
	if errors.Is(err, clientrunpod.ErrCreateAmbiguous) {
		markAmbiguousCreate(cr, desired.Name)
		e.recoverAmbiguousCreate(ctx, cr, desired, desired.Name)
		return managed.ExternalCreation{}, nil
	}
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	bindExternalID(cr, got.ID)
	setObservation(cr, got, true, e.template, false)
	return managed.ExternalCreation{}, nil
}

func (e *external) recoverAmbiguousCreate(ctx context.Context, cr *serverlessv1alpha1.Endpoint, desired clientrunpod.EndpointInput, name string) {
	desired.Name = name
	got, err := e.service.FindEndpointByName(ctx, name)
	if err != nil || !endpointMatchesInput(desired, got) || !endpointTemplateMatches(got, e.template) || !endpointWorkersMatch(desired, got, e.template, cr.Spec.ForProvider.MaxWorkerCostMilliUSDPerHour) {
		return
	}
	got, err = e.service.UpdateEndpoint(ctx, got.ID, desired)
	if err != nil || !endpointMatchesInput(desired, got) || !endpointTemplateMatches(got, e.template) || !endpointWorkersMatch(desired, got, e.template, cr.Spec.ForProvider.MaxWorkerCostMilliUSDPerHour) {
		return
	}
	bindExternalID(cr, got.ID)
	setObservation(cr, got, true, e.template, len(got.Workers) > 0)
}

func markAmbiguousCreate(cr *serverlessv1alpha1.Endpoint, name string) {
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[ambiguousCreateNameAnnotation] = name
}

func clearAmbiguousCreate(cr *serverlessv1alpha1.Endpoint) {
	delete(cr.Annotations, ambiguousCreateNameAnnotation)
}

func ambiguousAbsenceAcknowledged(cr *serverlessv1alpha1.Endpoint) bool {
	pending := cr.Annotations[externalCreatePendingAnnotation]
	return pending != "" && cr.Annotations[ambiguousAbsenceConfirmedAnnotation] == pending
}

func bindExternalID(cr *serverlessv1alpha1.Endpoint, id string) {
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	meta.SetExternalName(cr, id)
	cr.Status.AtProvider.ID = id
	cr.Annotations[externalIDBoundAnnotation] = "true"
	clearAmbiguousCreate(cr)
	meta.SetExternalCreateSucceeded(cr, time.Now())
}

func (e *external) expectedTemplateIDForCleanup(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (string, error) {
	ref := cr.Spec.ForProvider.TemplateIDRef
	if ref == nil {
		return "", nil
	}
	template := &serverlessv1alpha1.Template{}
	if err := e.reader.Get(ctx, types.NamespacedName{Namespace: cr.Namespace, Name: ref.Name}, template); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", errors.New("cannot read referenced Template during Endpoint cleanup recovery")
	}
	id := meta.GetExternalName(template)
	if id != "" && identifier.ValidateRunPodID(id) != nil {
		return "", errors.New("referenced Template has an invalid external ID")
	}
	return id, nil
}

func (e *external) Update(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalUpdate, error) {
	if err := validate(cr, e.template.id); err != nil {
		return managed.ExternalUpdate{}, err
	}
	if err := ensureLifetimeActive(cr, time.Now()); err != nil {
		return managed.ExternalUpdate{}, err
	}
	if identifier.ValidateRunPodID(meta.GetExternalName(cr)) != nil || cr.Status.AtProvider.ID != meta.GetExternalName(cr) {
		return managed.ExternalUpdate{}, errors.New("refusing to update an Endpoint without a persisted controller-observed ID binding")
	}
	current, err := e.service.GetEndpoint(ctx, meta.GetExternalName(cr))
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	if current == nil {
		return managed.ExternalUpdate{}, errors.New("RunPod Endpoint GET returned no object before update")
	}
	got, err := e.service.UpdateEndpoint(ctx, meta.GetExternalName(cr), endpointInput(cr, e.template.id))
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	setObservation(cr, got, true, e.template, false)
	return managed.ExternalUpdate{}, nil
}

func (e *external) Delete(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv2.Deleting())
	if identifier.ValidateRunPodID(meta.GetExternalName(cr)) != nil || cr.Status.AtProvider.ID != meta.GetExternalName(cr) {
		return managed.ExternalDelete{}, errors.New("refusing to delete an Endpoint without a persisted controller-observed ID binding")
	}
	checks := &verificationv1alpha1.EndpointCheckList{}
	if err := e.reader.List(ctx, checks, client.InNamespace(cr.Namespace)); err != nil {
		return managed.ExternalDelete{}, err
	}
	for i := range checks.Items {
		p := checks.Items[i].Spec.ForProvider
		if p.EndpointIDRef != nil && p.EndpointIDRef.Name == cr.Name {
			return managed.ExternalDelete{}, errors.New("EndpointCheck must be deleted before Endpoint so model-router removes the route")
		}
	}
	current, err := e.service.GetEndpoint(ctx, meta.GetExternalName(cr))
	if errors.Is(err, clientrunpod.ErrNotFound) {
		return managed.ExternalDelete{}, nil
	}
	if err != nil {
		return managed.ExternalDelete{}, err
	}
	if current == nil {
		return managed.ExternalDelete{}, errors.New("RunPod Endpoint GET returned no object before deletion")
	}
	err = e.service.DeleteEndpoint(ctx, meta.GetExternalName(cr))
	if errors.Is(err, clientrunpod.ErrNotFound) {
		err = nil
	}
	return managed.ExternalDelete{}, err
}

func (e *external) Disconnect(context.Context) error { return nil }

func resolveTemplateID(ctx context.Context, kube client.Reader, cr *serverlessv1alpha1.Endpoint) (string, error) {
	binding, err := resolveTemplate(ctx, kube, cr)
	return binding.id, err
}

func resolveTemplate(ctx context.Context, kube client.Reader, cr *serverlessv1alpha1.Endpoint) (templateBinding, error) {
	// A terminating Endpoint must be able to reach ExternalDelete even when its
	// referenced Template was deleted first or is no longer Ready. Deletion only
	// needs the RunPod endpoint external name and authenticated service client.
	if !cr.DeletionTimestamp.IsZero() {
		return templateBinding{}, nil
	}
	p := cr.Spec.ForProvider
	if p.TemplateIDRef == nil {
		return templateBinding{}, errors.New("templateIdRef is required; direct external Template IDs bypass the bounded Template contract")
	}
	t := &serverlessv1alpha1.Template{}
	if err := kube.Get(ctx, types.NamespacedName{Name: p.TemplateIDRef.Name, Namespace: cr.Namespace}, t); err != nil {
		return templateBinding{}, err
	}
	if !t.DeletionTimestamp.IsZero() || t.Status.ObservedGeneration != t.Generation ||
		t.Status.GetCondition(xpv2.TypeReady).Status != corev1.ConditionTrue ||
		t.Status.GetCondition(xpv2.TypeSynced).Status != corev1.ConditionTrue {
		return templateBinding{}, errors.New("referenced Template is not Ready for its current generation")
	}
	templateDeadline := t.CreationTimestamp.Add(time.Duration(t.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if !t.CreationTimestamp.IsZero() && !time.Now().Before(templateDeadline) {
		return templateBinding{}, errors.New("referenced Template maximum lifetime has elapsed")
	}
	if !t.CreationTimestamp.IsZero() && !cr.CreationTimestamp.IsZero() {
		endpointDeadline := cr.CreationTimestamp.Add(time.Duration(cr.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
		if endpointDeadline.After(templateDeadline) {
			return templateBinding{}, errors.New("Endpoint lifetime may not outlast its referenced Template")
		}
	}
	id := meta.GetExternalName(t)
	if id == "" || identifier.ValidateRunPodID(id) != nil || t.Status.AtProvider.ID != id {
		return templateBinding{}, errors.New("referenced Template external-name is not bound to its controller-observed ID")
	}
	containerDiskGB := defaultContainerDiskInGB
	if t.Spec.ForProvider.ContainerDiskInGB != nil {
		containerDiskGB = *t.Spec.ForProvider.ContainerDiskInGB
	}
	return templateBinding{
		id: id, uid: string(t.UID), generation: t.Generation, image: t.Spec.ForProvider.ImageName, name: t.Status.AtProvider.Name,
		containerDiskGB: containerDiskGB, entrypoint: slices.Clone(t.Spec.ForProvider.DockerEntrypoint),
		startCmd: slices.Clone(t.Spec.ForProvider.DockerStartCmd), ports: slices.Clone(t.Spec.ForProvider.Ports),
	}, nil
}

func validate(cr *serverlessv1alpha1.Endpoint, templateID string) error {
	p := cr.Spec.ForProvider
	if !safeEndpointName.MatchString(p.Name) {
		return errors.New("name must use bounded ASCII identifier characters")
	}
	if templateID == "" || len(p.GPUTypeIDs) == 0 || len(p.DataCenterIDs) == 0 {
		return errors.New("template ID, GPU type, and data center allowlists are required")
	}
	if p.WorkersMin != 1 || p.WorkersMax != 1 {
		return errors.New("secured endpoints require workersMin and workersMax exactly one")
	}
	if p.MaxWorkerCostMilliUSDPerHour <= 0 || p.MaxWorkerCostMilliUSDPerHour > 10000 {
		return errors.New("maxWorkerCostMilliUsdPerHour must be between 1 and 10000")
	}
	return nil
}

func ensureLifetimeActive(cr *serverlessv1alpha1.Endpoint, now time.Time) error {
	// Kubernetes always sets CreationTimestamp before reconciliation. Keep the
	// zero check for isolated callers and tests that construct an object without
	// API-server defaulting.
	if cr.CreationTimestamp.IsZero() {
		return nil
	}
	deadline := cr.CreationTimestamp.Add(time.Duration(cr.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if !now.Before(deadline) {
		return errors.New("Endpoint maximum lifetime has elapsed; refusing external create or update")
	}
	return nil
}

func endpointInput(cr *serverlessv1alpha1.Endpoint, templateID string) clientrunpod.EndpointInput {
	p := cr.Spec.ForProvider
	return clientrunpod.EndpointInput{
		Name: effectiveName(cr), TemplateID: templateID, ComputeType: "GPU", GPUCount: 1,
		GPUTypeIDs: p.GPUTypeIDs, AllowedCUDAVersions: p.AllowedCUDAVersions,
		DataCenterIDs: p.DataCenterIDs, WorkersMin: p.WorkersMin, WorkersMax: p.WorkersMax,
		IdleTimeout: p.IdleTimeout, ScalerType: p.ScalerType, ScalerValue: p.ScalerValue,
		ExecutionTimeoutMS: p.ExecutionTimeoutMS, FlashBoot: false,
		NetworkVolumeID: "", NetworkVolumeIDs: []string{}, MinCUDAVersion: "",
	}
}

func effectiveName(cr *serverlessv1alpha1.Endpoint) string {
	suffix := "-xp-" + strings.ToLower(string(cr.UID))
	maxPrefix := 191 - len(suffix)
	name := cr.Spec.ForProvider.Name
	if len(name) > maxPrefix {
		name = name[:maxPrefix]
	}
	return name + suffix
}

func endpointMatchesInput(want clientrunpod.EndpointInput, got *clientrunpod.Endpoint) bool {
	if got == nil {
		return false
	}
	return got.Name == want.Name && got.TemplateID == want.TemplateID && got.ComputeType == "GPU" &&
		got.GPUCount == 1 && got.WorkersMin != nil && *got.WorkersMin == want.WorkersMin &&
		got.WorkersMax != nil && *got.WorkersMax == want.WorkersMax && got.Version != nil &&
		got.IdleTimeout == want.IdleTimeout && got.ScalerType == want.ScalerType &&
		got.ScalerValue == want.ScalerValue && got.ExecutionTimeoutMS == want.ExecutionTimeoutMS &&
		(got.FlashBoot == nil || *got.FlashBoot == want.FlashBoot) &&
		got.NetworkVolumeID.Present && (got.NetworkVolumeID.Null || got.NetworkVolumeID.Value == "") &&
		got.NetworkVolumeIDs != nil && len(got.NetworkVolumeIDs) == 0 &&
		got.MinCUDAVersion.Present && (got.MinCUDAVersion.Null || got.MinCUDAVersion.Value == "") &&
		got.Env != nil && len(got.Env) == 0 && len(got.InstanceIDs) == 0 &&
		slices.Equal(got.GPUTypeIDs, want.GPUTypeIDs) &&
		got.AllowedCUDAVersions != nil && slices.Equal(got.AllowedCUDAVersions, want.AllowedCUDAVersions) &&
		slices.Equal(got.DataCenterIDs, want.DataCenterIDs)
}

func endpointTemplateMatches(got *clientrunpod.Endpoint, binding templateBinding) bool {
	if got == nil || got.Template == nil {
		return false
	}
	template := got.Template
	if template.ID != binding.id || template.Name != binding.name || template.Category != "NVIDIA" ||
		template.ImageName != binding.image || template.IsPublic == nil || *template.IsPublic ||
		template.IsRunpod == nil || *template.IsRunpod || template.IsServerless == nil || !*template.IsServerless ||
		template.ContainerDiskInGB == nil || *template.ContainerDiskInGB != binding.containerDiskGB ||
		template.VolumeInGB == nil || *template.VolumeInGB != 0 || template.Env == nil || len(template.Env) != 0 ||
		!template.ContainerRegistryAuthID.Present || (!template.ContainerRegistryAuthID.Null && template.ContainerRegistryAuthID.Value != "") ||
		!template.Readme.Present || template.Readme.Null || template.Readme.Value != "" ||
		!template.VolumeMountPath.Present || template.VolumeMountPath.Null || template.VolumeMountPath.Value != "" ||
		template.DockerEntrypoint == nil || !slices.Equal(template.DockerEntrypoint, binding.entrypoint) ||
		template.DockerStartCmd == nil || !slices.Equal(template.DockerStartCmd, binding.startCmd) ||
		template.Ports == nil || !slices.Equal(template.Ports, binding.ports) {
		return false
	}
	return true
}

func endpointWorkersMatch(want clientrunpod.EndpointInput, got *clientrunpod.Endpoint, template templateBinding, maxWorkerCostMilliUSDPerHour int32) bool {
	if got == nil || got.Workers == nil || got.WorkersMax == nil || int32(len(got.Workers)) > *got.WorkersMax || int32(len(got.Workers)) > want.WorkersMax {
		return false
	}
	maxCost := float64(maxWorkerCostMilliUSDPerHour) / 1000
	for i := range got.Workers {
		worker := &got.Workers[i]
		if !worker.ID.Present || worker.ID.Null ||
			!worker.EndpointID.Present || (!worker.EndpointID.Null && worker.EndpointID.Value != got.ID) ||
			!worker.TemplateID.Present || (!worker.TemplateID.Null && worker.TemplateID.Value != got.TemplateID) ||
			worker.SLSVersion == nil || got.Version == nil || *worker.SLSVersion != *got.Version ||
			!worker.Image.Present || worker.Image.Null || worker.Image.Value != template.image ||
			worker.ContainerDiskInGB == nil || *worker.ContainerDiskInGB != template.containerDiskGB ||
			!worker.ContainerRegistryAuthID.Present || (!worker.ContainerRegistryAuthID.Null && worker.ContainerRegistryAuthID.Value != "") ||
			worker.DockerEntrypoint == nil || !slices.Equal(worker.DockerEntrypoint, template.entrypoint) ||
			worker.DockerStartCmd == nil || !slices.Equal(worker.DockerStartCmd, template.startCmd) ||
			worker.Ports == nil || !slices.Equal(worker.Ports, template.ports) ||
			worker.Env == nil || len(worker.Env) != 0 || worker.VolumeInGB == nil || *worker.VolumeInGB != 0 ||
			!worker.VolumeMountPath.Present || (!worker.VolumeMountPath.Null && worker.VolumeMountPath.Value != "") ||
			// volumeEncrypted is an observed local-volume property. The bounded
			// contract requires an explicit zero-sized local volume, so either
			// documented boolean value is inert; omission remains fail closed.
			worker.VolumeEncrypted == nil ||
			worker.NetworkVolume == nil || string(bytes.TrimSpace(worker.NetworkVolume)) != "null" ||
			!worker.PublicIP.Present || (!worker.PublicIP.Null && worker.PublicIP.Value != "") ||
			!emptyJSONContainerOrNull(worker.PortMappings) || !emptyJSONContainerOrNull(worker.SavingsPlans) ||
			!worker.DesiredStatus.Present || worker.DesiredStatus.Null || worker.DesiredStatus.Value != "RUNNING" ||
			worker.Interruptible == nil || *worker.Interruptible ||
			!boundedCost(worker.AdjustedCostPerHr, maxCost) || !boundedCost(worker.CostPerHr, maxCost) ||
			worker.Locked == nil || *worker.Locked || worker.GPU == nil || worker.GPU.Count == nil || *worker.GPU.Count != 1 ||
			worker.Machine == nil || worker.Machine.GPUTypeID == nil || !slices.Contains(want.GPUTypeIDs, *worker.Machine.GPUTypeID) ||
			worker.Machine.DataCenterID == nil || !slices.Contains(want.DataCenterIDs, *worker.Machine.DataCenterID) ||
			worker.Machine.SecureCloud == nil || !*worker.Machine.SecureCloud ||
			!boundedCost(worker.Machine.CostPerHr, maxCost) || !boundedCost(worker.Machine.CurrentPricePerGPU, maxCost) {
			return false
		}
	}
	return true
}

func boundedCost(value clientrunpod.NullableDecimal, maximum float64) bool {
	return value.Present && !value.Null && !math.IsNaN(value.Value) && !math.IsInf(value.Value, 0) && value.Value >= 0 && value.Value <= maximum
}

func emptyJSONContainerOrNull(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}
	if bytes.Equal(trimmed, []byte("null")) {
		return true
	}
	switch trimmed[0] {
	case '[':
		var values []json.RawMessage
		return json.Unmarshal(trimmed, &values) == nil && len(values) == 0
	case '{':
		var values map[string]json.RawMessage
		return json.Unmarshal(trimmed, &values) == nil && len(values) == 0
	default:
		return false
	}
}

func unrepairableExternalState(got *clientrunpod.Endpoint, want clientrunpod.EndpointInput) error {
	if got.ComputeType != "GPU" {
		return errors.New("RunPod Endpoint computeType is not GPU and the official update API cannot change it")
	}
	if got.Env == nil {
		return errors.New("RunPod Endpoint response omitted environment evidence that the official update API cannot establish")
	}
	if len(got.Env) != 0 {
		return errors.New("RunPod Endpoint has unmanaged environment variables that the official update API cannot clear")
	}
	if got.ComputeType == "GPU" && len(got.InstanceIDs) != 0 {
		return errors.New("RunPod GPU Endpoint has CPU-only instanceIds that the official update API cannot clear")
	}
	if !got.NetworkVolumeID.Present || got.NetworkVolumeIDs == nil || !got.MinCUDAVersion.Present || got.AllowedCUDAVersions == nil {
		return errors.New("RunPod Endpoint response omitted bounded storage or CUDA evidence; refusing a rolling PATCH loop")
	}
	if (!got.NetworkVolumeID.Null && got.NetworkVolumeID.Value != "") || len(got.NetworkVolumeIDs) != 0 {
		return errors.New("RunPod Endpoint has attached persistent network storage and the official update API documents no safe clear sentinel")
	}
	if !got.MinCUDAVersion.Null && got.MinCUDAVersion.Value != "" {
		return errors.New("RunPod Endpoint has a minimum CUDA override and the official update API documents no safe clear sentinel")
	}
	if len(want.AllowedCUDAVersions) == 0 && len(got.AllowedCUDAVersions) != 0 {
		return errors.New("RunPod Endpoint has an allowed CUDA override and an empty-array clear is not part of the bounded wire contract")
	}
	return nil
}

// setObservation records the external state and returns whether flashboot=false
// has a current proof. The v1 GET schema omits FlashBoot, so a successful
// create/update acknowledgement is retained only for the same external ID and
// rollout Version. Adoption or a version change must pass through Update before
// the Endpoint can become Ready again.
func setObservation(cr *serverlessv1alpha1.Endpoint, got *clientrunpod.Endpoint, writeConfirmed bool, template templateBinding, activeWorkersProven bool) (bool, bool) {
	if got.WorkersMin == nil || got.WorkersMax == nil || got.Version == nil {
		cr.Status.ObservedGeneration = 0
		return false, false
	}
	version := *got.Version
	previous := cr.Status.AtProvider
	now := time.Now()
	flashBootDisabled := false
	evidenceAt := metav1.Time{}
	if got.FlashBoot != nil {
		flashBootDisabled = !*got.FlashBoot
	} else if writeConfirmed {
		flashBootDisabled = true
	} else if previous.ID == got.ID && previous.FlashBootEvidenceCurrent(now) && previous.FlashBootEvidenceVersion != nil && *previous.FlashBootEvidenceVersion == version {
		flashBootDisabled = true
	}
	if flashBootDisabled {
		if !writeConfirmed && got.FlashBoot == nil && previous.ID == got.ID && previous.FlashBootEvidenceCurrent(now) && previous.FlashBootEvidenceVersion != nil && *previous.FlashBootEvidenceVersion == version {
			evidenceAt = previous.FlashBootLastEnforcedAt
		} else {
			evidenceAt = metav1.NewTime(now)
		}
	}
	templateRevisionProven := previous.TemplateResourceUID == template.uid &&
		previous.TemplateResourceGeneration == template.generation && previous.TemplateImageDigest == template.image
	if previous.TemplateResourceUID == "" && writeConfirmed {
		templateRevisionProven = true
	}
	if previous.TemplateResourceUID != "" && previous.Version != nil && *previous.Version != version {
		templateRevisionProven = true
	}
	boundTemplateUID := previous.TemplateResourceUID
	boundTemplateGeneration := previous.TemplateResourceGeneration
	boundTemplateImage := previous.TemplateImageDigest
	if templateRevisionProven {
		boundTemplateUID, boundTemplateGeneration, boundTemplateImage = template.uid, template.generation, template.image
	}
	workerValidated := false
	workerObservedAt := metav1.Time{}
	var workerProofVersion *int32
	if activeWorkersProven {
		workerValidated = true
		workerObservedAt = metav1.NewTime(now)
		workerVersion := version
		workerProofVersion = &workerVersion
	}
	cr.Status.AtProvider = serverlessv1alpha1.EndpointObservation{
		ID: got.ID, TemplateID: got.TemplateID, Version: &version, WorkersMin: *got.WorkersMin,
		TemplateResourceUID: boundTemplateUID, TemplateResourceGeneration: boundTemplateGeneration, TemplateImageDigest: boundTemplateImage,
		FlashBootDisabled: flashBootDisabled, FlashBootLastEnforcedAt: evidenceAt,
		WorkerSecurityValidated: workerValidated, WorkerSecurityProofVersion: workerProofVersion, WorkerSecurityObservedAt: workerObservedAt,
		WorkersMax: *got.WorkersMax, InferenceURL: fmt.Sprintf("https://api.runpod.ai/v2/%s/openai/v1", got.ID),
		LastObservedAt: metav1.NewTime(now),
	}
	if flashBootDisabled {
		cr.Status.AtProvider.FlashBootEvidenceVersion = &version
	}
	return flashBootDisabled, templateRevisionProven
}
