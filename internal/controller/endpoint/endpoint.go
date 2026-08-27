// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package endpoint

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	verificationv1alpha1 "github.com/torrescd/provider-runpod/apis/verification/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
	"github.com/torrescd/provider-runpod/internal/controller/management"
)

type service interface {
	GetEndpoint(context.Context, string) (*clientrunpod.Endpoint, error)
	FindEndpointByName(context.Context, string) (*clientrunpod.Endpoint, error)
	CreateEndpoint(context.Context, clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error)
	UpdateEndpoint(context.Context, string, clientrunpod.EndpointInput) (*clientrunpod.Endpoint, error)
	DeleteEndpoint(context.Context, string) error
}

func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(xperrors.Wrap(err, "cannot setup Endpoint controller"))
		}
	}, serverlessv1alpha1.EndpointGroupVersionKind)
	return nil
}

func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(serverlessv1alpha1.EndpointGroupKind)
	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*serverlessv1alpha1.Endpoint](&connector{
			kube:  mgr.GetClient(),
			usage: resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))), //nolint:staticcheck
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
		WithEventFilter(resource.DesiredStateChanged()).
		For(&serverlessv1alpha1.Endpoint{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube  client.Client
	usage *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.TypedExternalClient[*serverlessv1alpha1.Endpoint], error) {
	svc, err := management.Connect(ctx, c.kube, c.usage, cr)
	if err != nil {
		return nil, err
	}
	templateID, err := resolveTemplateID(ctx, c.kube, cr)
	if err != nil {
		return nil, err
	}
	return &external{service: svc, kube: c.kube, templateID: templateID}, nil
}

type external struct {
	service    service
	kube       client.Client
	templateID string
}

func (e *external) Observe(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalObservation, error) {
	id := meta.GetExternalName(cr)
	if id == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	got, err := e.service.GetEndpoint(ctx, id)
	if errors.Is(err, clientrunpod.ErrNotFound) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	setObservation(cr, got)
	upToDate := endpointMatchesInput(endpointInput(cr, e.templateID), got)
	if upToDate {
		cr.Status.SetConditions(xpv2.Available())
	}
	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: upToDate}, nil
}

func (e *external) Create(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv2.Creating())
	if err := validate(cr, e.templateID); err != nil {
		return managed.ExternalCreation{}, err
	}
	desired := endpointInput(cr, e.templateID)
	got, err := e.service.CreateEndpoint(ctx, desired)
	if errors.Is(err, clientrunpod.ErrCreateAmbiguous) {
		got, err = e.service.FindEndpointByName(ctx, desired.Name)
		if err == nil && !endpointMatchesInput(desired, got) {
			return managed.ExternalCreation{}, clientrunpod.ErrAmbiguous
		}
	}
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	meta.SetExternalName(cr, got.ID)
	setObservation(cr, got)
	return managed.ExternalCreation{}, nil
}

func (e *external) Update(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalUpdate, error) {
	if err := validate(cr, e.templateID); err != nil {
		return managed.ExternalUpdate{}, err
	}
	got, err := e.service.UpdateEndpoint(ctx, meta.GetExternalName(cr), endpointInput(cr, e.templateID))
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	setObservation(cr, got)
	return managed.ExternalUpdate{}, nil
}

func (e *external) Delete(ctx context.Context, cr *serverlessv1alpha1.Endpoint) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv2.Deleting())
	checks := &verificationv1alpha1.EndpointCheckList{}
	if err := e.kube.List(ctx, checks, client.InNamespace(cr.Namespace)); err != nil {
		return managed.ExternalDelete{}, err
	}
	for i := range checks.Items {
		p := checks.Items[i].Spec.ForProvider
		if p.EndpointID == meta.GetExternalName(cr) || (p.EndpointIDRef != nil && p.EndpointIDRef.Name == cr.Name) {
			return managed.ExternalDelete{}, errors.New("EndpointCheck must be deleted before Endpoint so model-router removes the route")
		}
	}
	err := e.service.DeleteEndpoint(ctx, meta.GetExternalName(cr))
	if errors.Is(err, clientrunpod.ErrNotFound) {
		err = nil
	}
	return managed.ExternalDelete{}, err
}

func (e *external) Disconnect(context.Context) error { return nil }

func resolveTemplateID(ctx context.Context, kube client.Client, cr *serverlessv1alpha1.Endpoint) (string, error) {
	p := cr.Spec.ForProvider
	if p.TemplateIDRef == nil {
		if p.TemplateID == "" {
			return "", errors.New("one of templateId or templateIdRef is required")
		}
		return p.TemplateID, nil
	}
	if p.TemplateID != "" {
		return "", errors.New("templateId and templateIdRef are mutually exclusive")
	}
	t := &serverlessv1alpha1.Template{}
	if err := kube.Get(ctx, types.NamespacedName{Name: p.TemplateIDRef.Name, Namespace: cr.Namespace}, t); err != nil {
		return "", err
	}
	id := meta.GetExternalName(t)
	if id == "" {
		return "", errors.New("referenced Template does not yet have an external ID")
	}
	return id, nil
}

func validate(cr *serverlessv1alpha1.Endpoint, templateID string) error {
	p := cr.Spec.ForProvider
	if templateID == "" || len(p.GPUTypeIDs) == 0 {
		return errors.New("template ID and at least one GPU type are required")
	}
	if p.WorkersMax < 0 || p.WorkersMax > 1 {
		return errors.New("workersMax must be zero or one")
	}
	return nil
}

func endpointInput(cr *serverlessv1alpha1.Endpoint, templateID string) clientrunpod.EndpointInput {
	p := cr.Spec.ForProvider
	return clientrunpod.EndpointInput{
		Name: effectiveName(cr), TemplateID: templateID, ComputeType: "GPU", GPUCount: 1,
		GPUTypeIDs: p.GPUTypeIDs, AllowedCUDAVersions: p.AllowedCUDAVersions,
		DataCenterIDs: p.DataCenterIDs, WorkersMin: 0, WorkersMax: p.WorkersMax,
		IdleTimeout: p.IdleTimeout, ScalerType: p.ScalerType, ScalerValue: p.ScalerValue,
		ExecutionTimeoutMS: p.ExecutionTimeoutMS,
	}
}

func effectiveName(cr *serverlessv1alpha1.Endpoint) string {
	suffix := "-xp-" + strings.ToLower(string(cr.UID))
	if len(suffix) > 16 {
		suffix = suffix[:16]
	}
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
		got.GPUCount == 1 && got.WorkersMin == 0 && got.WorkersMax == want.WorkersMax &&
		got.IdleTimeout == want.IdleTimeout && got.ScalerType == want.ScalerType &&
		got.ScalerValue == want.ScalerValue && got.ExecutionTimeoutMS == want.ExecutionTimeoutMS &&
		slices.Equal(got.GPUTypeIDs, want.GPUTypeIDs) &&
		(want.AllowedCUDAVersions == nil || slices.Equal(got.AllowedCUDAVersions, want.AllowedCUDAVersions)) &&
		(want.DataCenterIDs == nil || slices.Equal(got.DataCenterIDs, want.DataCenterIDs))
}

func setObservation(cr *serverlessv1alpha1.Endpoint, got *clientrunpod.Endpoint) {
	cr.Status.AtProvider = serverlessv1alpha1.EndpointObservation{
		ID: got.ID, TemplateID: got.TemplateID, WorkersMin: got.WorkersMin,
		WorkersMax:     got.WorkersMax,
		InferenceURL:   fmt.Sprintf("https://api.runpod.ai/v2/%s/openai/v1", got.ID),
		LastObservedAt: metav1.Now(),
	}
}
