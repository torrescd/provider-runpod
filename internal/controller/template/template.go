// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package template

import (
	"context"
	"errors"
	"regexp"
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
	"github.com/torrescd/provider-runpod/internal/controller/management"
)

var digestImage = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)

type service interface {
	GetTemplate(context.Context, string) (*clientrunpod.Template, error)
	FindTemplateByName(context.Context, string) (*clientrunpod.Template, error)
	CreateTemplate(context.Context, clientrunpod.TemplateInput) (*clientrunpod.Template, error)
	UpdateTemplate(context.Context, string, clientrunpod.TemplateInput) (*clientrunpod.Template, error)
	DeleteTemplate(context.Context, string) error
}

func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(xperrors.Wrap(err, "cannot setup Template controller"))
		}
	}, serverlessv1alpha1.TemplateGroupVersionKind)
	return nil
}

func Setup(mgr ctrl.Manager, o controller.Options) error {
	name := managed.ControllerName(serverlessv1alpha1.TemplateGroupKind)
	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*serverlessv1alpha1.Template](&connector{
			kube:   mgr.GetClient(),
			reader: mgr.GetAPIReader(),
			usage:  resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
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
			recorder := statemetrics.NewMRStateRecorder(mgr.GetClient(), o.Logger, o.MetricOptions.MRStateMetrics, &serverlessv1alpha1.TemplateList{}, o.MetricOptions.PollStateMetricInterval)
			if err := mgr.Add(recorder); err != nil {
				return err
			}
		}
	}
	r := managed.NewReconciler(mgr, resource.ManagedKind(serverlessv1alpha1.TemplateGroupVersionKind), opts...)
	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&serverlessv1alpha1.Template{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

type connector struct {
	kube   client.Client
	reader client.Reader
	usage  *resource.ProviderConfigUsageTracker
}

func (c *connector) Connect(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.TypedExternalClient[*serverlessv1alpha1.Template], error) {
	svc, err := management.Connect(ctx, c.reader, c.usage, cr)
	if err != nil {
		return nil, err
	}
	return &external{service: svc}, nil
}

type external struct{ service service }

func (e *external) Observe(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.ExternalObservation, error) {
	id := meta.GetExternalName(cr)
	if id == "" {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	got, err := e.service.GetTemplate(ctx, id)
	if errors.Is(err, clientrunpod.ErrNotFound) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	setObservation(cr, got)
	upToDate := templateMatchesInput(templateInput(cr), got)
	if upToDate {
		cr.Status.SetConditions(xpv2.Available())
	}
	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: upToDate}, nil
}

func (e *external) Create(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv2.Creating())
	if !digestImage.MatchString(cr.Spec.ForProvider.ImageName) {
		return managed.ExternalCreation{}, errors.New("imageName must be pinned to a lowercase sha256 OCI digest")
	}
	desired := templateInput(cr)
	got, err := e.service.CreateTemplate(ctx, desired)
	if errors.Is(err, clientrunpod.ErrCreateAmbiguous) {
		got, err = e.service.FindTemplateByName(ctx, desired.Name)
		if err == nil && !templateMatchesInput(desired, got) {
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

func (e *external) Update(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.ExternalUpdate, error) {
	if !digestImage.MatchString(cr.Spec.ForProvider.ImageName) {
		return managed.ExternalUpdate{}, errors.New("imageName must be pinned to a lowercase sha256 OCI digest")
	}
	got, err := e.service.UpdateTemplate(ctx, meta.GetExternalName(cr), templateInput(cr))
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	setObservation(cr, got)
	return managed.ExternalUpdate{}, nil
}

func (e *external) Delete(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.ExternalDelete, error) {
	cr.Status.SetConditions(xpv2.Deleting())
	err := e.service.DeleteTemplate(ctx, meta.GetExternalName(cr))
	if errors.Is(err, clientrunpod.ErrNotFound) {
		err = nil
	}
	return managed.ExternalDelete{}, err
}

func (e *external) Disconnect(context.Context) error { return nil }

func templateInput(cr *serverlessv1alpha1.Template) clientrunpod.TemplateInput {
	p := cr.Spec.ForProvider
	return clientrunpod.TemplateInput{
		Name: effectiveName(cr), ImageName: p.ImageName, IsPublic: false, IsServerless: true,
		ContainerDiskInGB: p.ContainerDiskInGB, DockerEntrypoint: p.DockerEntrypoint,
		DockerStartCmd: p.DockerStartCmd, Ports: p.Ports, VolumeInGB: p.VolumeInGB,
		VolumeMountPath: p.VolumeMountPath,
	}
}

func effectiveName(cr *serverlessv1alpha1.Template) string {
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

func templateMatchesInput(want clientrunpod.TemplateInput, got *clientrunpod.Template) bool {
	if got == nil || got.Name != want.Name || got.ImageName != want.ImageName || got.IsPublic || !got.IsServerless {
		return false
	}
	if want.ContainerDiskInGB != nil && (got.ContainerDiskInGB == nil || *got.ContainerDiskInGB != *want.ContainerDiskInGB) {
		return false
	}
	if want.VolumeInGB != nil && (got.VolumeInGB == nil || *got.VolumeInGB != *want.VolumeInGB) {
		return false
	}
	return (want.DockerEntrypoint == nil || slices.Equal(want.DockerEntrypoint, got.DockerEntrypoint)) &&
		(want.DockerStartCmd == nil || slices.Equal(want.DockerStartCmd, got.DockerStartCmd)) &&
		(want.Ports == nil || slices.Equal(want.Ports, got.Ports)) &&
		(want.VolumeMountPath == "" || want.VolumeMountPath == got.VolumeMountPath)
}

func setObservation(cr *serverlessv1alpha1.Template, got *clientrunpod.Template) {
	cr.Status.AtProvider = serverlessv1alpha1.TemplateObservation{
		ID: got.ID, Name: got.Name, ImageName: got.ImageName,
		IsServerless: got.IsServerless, LastObservedAt: metav1.Now(),
	}
}
