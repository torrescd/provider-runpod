// Copyright 2026 The provider-runpod Authors.
// Licensed under the Apache License, Version 2.0.

package template

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	serverlessv1alpha1 "github.com/torrescd/provider-runpod/apis/serverless/v1alpha1"
	apisv1alpha1 "github.com/torrescd/provider-runpod/apis/v1alpha1"
	clientrunpod "github.com/torrescd/provider-runpod/internal/client/runpod"
	"github.com/torrescd/provider-runpod/internal/controller/management"
	"github.com/torrescd/provider-runpod/internal/identifier"
)

var (
	digestImage = regexp.MustCompile(`^[^\s@]+@sha256:[a-f0-9]{64}$`)
	portSpec    = regexp.MustCompile(`^[1-9][0-9]{0,4}/(http|tcp)$`)
	safeName    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
)

const (
	ambiguousCreateNameAnnotation       = "template.serverless.runpod.crossplane.io/ambiguous-create-name"
	externalIDBoundAnnotation           = "template.serverless.runpod.crossplane.io/external-id-bound"
	ambiguousAbsenceConfirmedAnnotation = "runpod.crossplane.io/ambiguous-create-absence-confirmed"
	externalCreatePendingAnnotation     = "crossplane.io/external-create-pending"
)

const defaultContainerDiskInGB int32 = 50

type service interface {
	GetTemplate(context.Context, string) (*clientrunpod.Template, error)
	FindTemplateByName(context.Context, string) (*clientrunpod.Template, error)
	CreateTemplate(context.Context, clientrunpod.TemplateInput) (*clientrunpod.Template, error)
	UpdateTemplate(context.Context, string, clientrunpod.TemplateInput) (*clientrunpod.Template, error)
	DeleteTemplate(context.Context, string) error
	ListEndpoints(context.Context) ([]clientrunpod.Endpoint, error)
}

func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := Setup(mgr, o); err != nil {
			panic(xperrors.Wrap(err, "cannot setup Template controller"))
		}
	}, serverlessv1alpha1.TemplateGroupVersionKind, serverlessv1alpha1.EndpointGroupVersionKind)
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
	return &external{service: svc, reader: c.reader}, nil
}

type external struct {
	service service
	reader  client.Reader
}

func (e *external) Observe(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.ExternalObservation, error) {
	if cr.DeletionTimestamp.IsZero() && cr.Annotations[ambiguousAbsenceConfirmedAnnotation] != "" {
		return managed.ExternalObservation{}, errors.New("ambiguous Template absence is acknowledged; delete the CR to finalize cleanup; recreation is disabled")
	}
	pendingName := cr.Annotations[ambiguousCreateNameAnnotation]
	if meta.ExternalCreateIncomplete(cr) {
		pendingName = effectiveName(cr)
	}
	if pendingName != "" {
		if pendingName != effectiveName(cr) {
			return managed.ExternalObservation{}, errors.New("Template recovery marker is not the controller-owned UID-scoped name")
		}
		got, err := e.service.FindTemplateByName(ctx, pendingName)
		if err != nil {
			if !cr.DeletionTimestamp.IsZero() && errors.Is(err, clientrunpod.ErrNotFound) {
				if ambiguousAbsenceAcknowledged(cr) {
					return managed.ExternalObservation{ResourceExists: false}, nil
				}
				return managed.ExternalObservation{}, errors.New("ambiguous Template create absence is unproven; an exact pending-token operator acknowledgement is required before deletion can complete")
			}
			return managed.ExternalObservation{}, errors.New("ambiguous Template create recovery is pending; no new create is permitted")
		}
		if got.Name != pendingName || identifier.ValidateRunPodID(got.ID) != nil {
			return managed.ExternalObservation{}, errors.New("Template recovery returned an invalid external identity")
		}
		if !cr.DeletionTimestamp.IsZero() {
			bindExternalID(cr, got.ID)
			cr.Status.AtProvider.ID = got.ID
			cr.Status.AtProvider.Name = got.Name
			return managed.ExternalObservation{ResourceExists: true, ResourceLateInitialized: true}, nil
		}
		desired := templateInput(cr)
		desired.Name = pendingName
		if !templateMatchesInput(desired, got) {
			return managed.ExternalObservation{}, clientrunpod.ErrAmbiguous
		}
		bindExternalID(cr, got.ID)
		setObservation(cr, got)
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
		return managed.ExternalObservation{}, errors.New("Template external-name is invalid")
	}
	statusBound := cr.Status.AtProvider.ID != ""
	if statusBound && cr.Status.AtProvider.ID != id {
		return managed.ExternalObservation{}, errors.New("Template external-name differs from its persisted controller-observed ID")
	}
	got, err := e.service.GetTemplate(ctx, id)
	if errors.Is(err, clientrunpod.ErrNotFound) {
		if !cr.DeletionTimestamp.IsZero() {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.New("bound RunPod Template is missing; refusing to create a replacement under an immutable external-name")
	}
	if err != nil {
		return managed.ExternalObservation{}, err
	}
	if got == nil {
		return managed.ExternalObservation{}, errors.New("RunPod Template GET returned no object")
	}
	// crossplane-runtime persists critical create annotations separately and may
	// intentionally discard status mutations made by ExternalCreate. Recover
	// that crash boundary only when the durable marker is present and the
	// returned object has this CR's deterministic UID-scoped name. Once status
	// has been persisted, the ID remains authoritative so an external rename
	// cannot strand repair or cleanup.
	if !statusBound {
		if got.Name != effectiveName(cr) {
			return managed.ExternalObservation{}, errors.New("Template binding marker did not resolve to the controller-owned UID-scoped name")
		}
		cr.Status.AtProvider.ID = id
	}
	setObservation(cr, got)
	if err := unrepairableExternalState(got); err != nil {
		cr.Status.ObservedGeneration = 0
		return managed.ExternalObservation{}, err
	}
	upToDate := templateMatchesInput(templateInput(cr), got)
	if upToDate {
		cr.Status.ObservedGeneration = cr.Generation
		cr.Status.SetConditions(xpv2.Available())
	} else {
		// A prior Ready observation must not remain consumable while external
		// drift is awaiting Update and a confirming Observe.
		cr.Status.ObservedGeneration = 0
	}
	return managed.ExternalObservation{ResourceExists: true, ResourceUpToDate: upToDate}, nil
}

func unrepairableExternalState(got *clientrunpod.Template) error {
	if got.Category != "NVIDIA" {
		return errors.New("RunPod Template category is not NVIDIA and the official update API cannot change it")
	}
	if got.IsServerless == nil || !*got.IsServerless {
		return errors.New("RunPod Template is not Serverless and the official update API cannot change it")
	}
	if got.IsRunpod == nil || *got.IsRunpod {
		return errors.New("RunPod Template ownership is absent or RunPod-managed and the official update API cannot change it")
	}
	if got.IsPublic == nil || got.ContainerDiskInGB == nil || got.VolumeInGB == nil ||
		got.DockerEntrypoint == nil || got.DockerStartCmd == nil || got.Ports == nil || got.Env == nil ||
		!got.Readme.Present || !got.VolumeMountPath.Present || !got.ContainerRegistryAuthID.Present {
		return errors.New("RunPod Template response omitted bounded configuration evidence; refusing a rolling PATCH loop")
	}
	return nil
}

func (e *external) Create(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.ExternalCreation, error) {
	cr.Status.SetConditions(xpv2.Creating())
	if cr.Annotations[ambiguousAbsenceConfirmedAnnotation] != "" {
		return managed.ExternalCreation{}, errors.New("ambiguous Template absence acknowledgement forbids external creation; delete the CR to finalize")
	}
	if cr.UID == "" {
		return managed.ExternalCreation{}, errors.New("Kubernetes UID is required before creating a RunPod Template")
	}
	if !digestImage.MatchString(cr.Spec.ForProvider.ImageName) {
		return managed.ExternalCreation{}, errors.New("imageName must be pinned to a lowercase sha256 OCI digest")
	}
	if !safeName.MatchString(cr.Spec.ForProvider.Name) {
		return managed.ExternalCreation{}, errors.New("name must use bounded ASCII identifier characters")
	}
	if err := validatePorts(cr.Spec.ForProvider.Ports); err != nil {
		return managed.ExternalCreation{}, err
	}
	if err := ensureLifetimeActive(cr, time.Now()); err != nil {
		return managed.ExternalCreation{}, err
	}
	desired := templateInput(cr)
	if pendingName := cr.Annotations[ambiguousCreateNameAnnotation]; pendingName != "" {
		e.recoverAmbiguousCreate(ctx, cr, desired, pendingName)
		return managed.ExternalCreation{}, nil
	}
	got, err := e.service.CreateTemplate(ctx, desired)
	if errors.Is(err, clientrunpod.ErrCreateAmbiguous) {
		markAmbiguousCreate(cr, desired.Name)
		e.recoverAmbiguousCreate(ctx, cr, desired, desired.Name)
		// Success is intentional: crossplane-runtime persists annotations made by
		// Create and records external-create-succeeded. Observe now owns recovery
		// and will return an error rather than permit another POST until the exact
		// UID-scoped name is uniquely visible or an operator clears the marker.
		return managed.ExternalCreation{}, nil
	}
	if err != nil {
		return managed.ExternalCreation{}, err
	}
	bindExternalID(cr, got.ID)
	setObservation(cr, got)
	return managed.ExternalCreation{}, nil
}

func (e *external) recoverAmbiguousCreate(ctx context.Context, cr *serverlessv1alpha1.Template, desired clientrunpod.TemplateInput, name string) {
	desired.Name = name
	got, err := e.service.FindTemplateByName(ctx, name)
	if err != nil || !templateMatchesInput(desired, got) {
		return
	}
	bindExternalID(cr, got.ID)
	setObservation(cr, got)
}

func markAmbiguousCreate(cr *serverlessv1alpha1.Template, name string) {
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	cr.Annotations[ambiguousCreateNameAnnotation] = name
}

func clearAmbiguousCreate(cr *serverlessv1alpha1.Template) {
	delete(cr.Annotations, ambiguousCreateNameAnnotation)
}

func ambiguousAbsenceAcknowledged(cr *serverlessv1alpha1.Template) bool {
	pending := cr.Annotations[externalCreatePendingAnnotation]
	return pending != "" && cr.Annotations[ambiguousAbsenceConfirmedAnnotation] == pending
}

func bindExternalID(cr *serverlessv1alpha1.Template, id string) {
	if cr.Annotations == nil {
		cr.Annotations = map[string]string{}
	}
	meta.SetExternalName(cr, id)
	cr.Status.AtProvider.ID = id
	cr.Annotations[externalIDBoundAnnotation] = "true"
	clearAmbiguousCreate(cr)
	meta.SetExternalCreateSucceeded(cr, time.Now())
}

func (e *external) Update(ctx context.Context, cr *serverlessv1alpha1.Template) (managed.ExternalUpdate, error) {
	if !digestImage.MatchString(cr.Spec.ForProvider.ImageName) {
		return managed.ExternalUpdate{}, errors.New("imageName must be pinned to a lowercase sha256 OCI digest")
	}
	if !safeName.MatchString(cr.Spec.ForProvider.Name) {
		return managed.ExternalUpdate{}, errors.New("name must use bounded ASCII identifier characters")
	}
	if err := validatePorts(cr.Spec.ForProvider.Ports); err != nil {
		return managed.ExternalUpdate{}, err
	}
	if err := ensureLifetimeActive(cr, time.Now()); err != nil {
		return managed.ExternalUpdate{}, err
	}
	if identifier.ValidateRunPodID(meta.GetExternalName(cr)) != nil || cr.Status.AtProvider.ID != meta.GetExternalName(cr) {
		return managed.ExternalUpdate{}, errors.New("refusing to update a Template without a persisted controller-observed ID binding")
	}
	current, err := e.service.GetTemplate(ctx, meta.GetExternalName(cr))
	if err != nil {
		return managed.ExternalUpdate{}, err
	}
	if current == nil {
		return managed.ExternalUpdate{}, errors.New("RunPod Template GET returned no object before update")
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
	templateID := meta.GetExternalName(cr)
	if identifier.ValidateRunPodID(templateID) != nil || cr.Status.AtProvider.ID != templateID {
		return managed.ExternalDelete{}, errors.New("refusing to delete a Template without a persisted controller-observed ID binding")
	}
	endpoints := &serverlessv1alpha1.EndpointList{}
	if err := e.reader.List(ctx, endpoints, client.InNamespace(cr.Namespace)); err != nil {
		return managed.ExternalDelete{}, err
	}
	for i := range endpoints.Items {
		p := endpoints.Items[i].Spec.ForProvider
		if p.TemplateIDRef != nil && p.TemplateIDRef.Name == cr.Name {
			return managed.ExternalDelete{}, errors.New("Endpoint must be deleted before its Template")
		}
	}
	// Kubernetes references cover provider-managed objects. The authoritative
	// management API list additionally fails closed for an adopted or unmanaged
	// Endpoint that still binds this external Template.
	externalEndpoints, err := e.service.ListEndpoints(ctx)
	if err != nil {
		return managed.ExternalDelete{}, err
	}
	for i := range externalEndpoints {
		if identifier.ValidateRunPodID(externalEndpoints[i].TemplateID) != nil {
			return managed.ExternalDelete{}, errors.New("RunPod Endpoint list omitted or returned an invalid templateId; refusing Template deletion")
		}
		if externalEndpoints[i].TemplateID == templateID {
			return managed.ExternalDelete{}, errors.New("RunPod Endpoint must be deleted before its Template")
		}
	}
	current, err := e.service.GetTemplate(ctx, templateID)
	if errors.Is(err, clientrunpod.ErrNotFound) {
		return managed.ExternalDelete{}, nil
	}
	if err != nil {
		return managed.ExternalDelete{}, err
	}
	if current == nil {
		return managed.ExternalDelete{}, errors.New("RunPod Template GET returned no object before deletion")
	}
	err = e.service.DeleteTemplate(ctx, templateID)
	if errors.Is(err, clientrunpod.ErrNotFound) {
		err = nil
	}
	return managed.ExternalDelete{}, err
}

func (e *external) Disconnect(context.Context) error { return nil }

func ensureLifetimeActive(cr *serverlessv1alpha1.Template, now time.Time) error {
	if cr.CreationTimestamp.IsZero() {
		return nil
	}
	deadline := cr.CreationTimestamp.Add(time.Duration(cr.Spec.ForProvider.MaxLifetimeSeconds) * time.Second)
	if !now.Before(deadline) {
		return errors.New("Template maximum lifetime has elapsed; refusing external create or update")
	}
	return nil
}

func templateInput(cr *serverlessv1alpha1.Template) clientrunpod.TemplateInput {
	p := cr.Spec.ForProvider
	containerDiskInGB := defaultContainerDiskInGB
	if p.ContainerDiskInGB != nil {
		containerDiskInGB = *p.ContainerDiskInGB
	}
	entrypoint := slices.Clone(p.DockerEntrypoint)
	if entrypoint == nil {
		entrypoint = []string{}
	}
	startCmd := slices.Clone(p.DockerStartCmd)
	if startCmd == nil {
		startCmd = []string{}
	}
	return clientrunpod.TemplateInput{
		Name: effectiveName(cr), Category: "NVIDIA", ImageName: p.ImageName, IsPublic: false, IsServerless: true,
		ContainerDiskInGB: containerDiskInGB, DockerEntrypoint: entrypoint,
		DockerStartCmd: startCmd, Ports: slices.Clone(p.Ports), VolumeInGB: p.VolumeInGB,
		Readme: "", VolumeMountPath: "", Env: map[string]string{}, ContainerRegistryAuthID: "",
	}
}

func effectiveName(cr *serverlessv1alpha1.Template) string {
	suffix := "-xp-" + strings.ToLower(string(cr.UID))
	maxPrefix := 191 - len(suffix)
	name := cr.Spec.ForProvider.Name
	if len(name) > maxPrefix {
		name = name[:maxPrefix]
	}
	return name + suffix
}

func validatePorts(ports []string) error {
	if len(ports) == 0 {
		return errors.New("at least one explicit port is required")
	}
	for _, port := range ports {
		if !portSpec.MatchString(port) {
			return errors.New("ports must use explicit port/protocol declarations")
		}
		number, err := strconv.Atoi(strings.SplitN(port, "/", 2)[0])
		if err != nil || number < 1 || number > 65535 {
			return errors.New("ports must be in the range 1..65535")
		}
	}
	return nil
}

func templateMatchesInput(want clientrunpod.TemplateInput, got *clientrunpod.Template) bool {
	if got == nil || got.Category != "NVIDIA" || got.Name != want.Name || got.ImageName != want.ImageName ||
		got.IsPublic == nil || *got.IsPublic || got.IsRunpod == nil || *got.IsRunpod ||
		got.IsServerless == nil || !*got.IsServerless {
		return false
	}
	if got.ContainerDiskInGB == nil || *got.ContainerDiskInGB != want.ContainerDiskInGB {
		return false
	}
	// RunPod defaults an omitted value to a persistent 20 GiB volume. An absent
	// response field therefore cannot prove the bounded zero-volume state.
	if got.VolumeInGB == nil || *got.VolumeInGB != want.VolumeInGB {
		return false
	}
	return got.Readme.Present && !got.Readme.Null && got.Readme.Value == want.Readme &&
		got.VolumeMountPath.Present && !got.VolumeMountPath.Null && got.VolumeMountPath.Value == want.VolumeMountPath &&
		got.Env != nil && len(got.Env) == 0 && got.ContainerRegistryAuthID.Present &&
		(got.ContainerRegistryAuthID.Null || got.ContainerRegistryAuthID.Value == "") &&
		got.DockerEntrypoint != nil && slices.Equal(want.DockerEntrypoint, got.DockerEntrypoint) &&
		got.DockerStartCmd != nil && slices.Equal(want.DockerStartCmd, got.DockerStartCmd) &&
		got.Ports != nil && slices.Equal(want.Ports, got.Ports)
}

func setObservation(cr *serverlessv1alpha1.Template, got *clientrunpod.Template) {
	isServerless := got.IsServerless != nil && *got.IsServerless
	cr.Status.AtProvider = serverlessv1alpha1.TemplateObservation{
		ID: got.ID, Name: got.Name, ImageName: got.ImageName,
		IsServerless: isServerless, LastObservedAt: metav1.Now(),
	}
}
