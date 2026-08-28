# Declarative Terraform-to-Crossplane migration

This is a conceptual migration guide, not a conversion of Terraform provider
source or schema. Only declarative infrastructure concepts are mapped.
Imperative actions and data-source-style queries are intentionally excluded.

| Declarative concept | Crossplane API | Availability |
|---|---|---|
| Serverless template | `Template.serverless.runpod.crossplane.io/v1alpha1` | v0.1 |
| Serverless endpoint | `Endpoint.serverless.runpod.crossplane.io/v1alpha1` | v0.1 |
| Persistent pod | Planned `Pod.compute.runpod.crossplane.io` | Later |
| Network volume | Planned `NetworkVolume.storage.runpod.crossplane.io` | Later |

`EndpointCheck` is a new security/readiness resource with no Terraform
equivalent. It performs authenticated health, exact model identity, and tool-call
checks before routing. Its controller runs in the separate model-router process,
not in the management provider process, and accepts only an
inference-purpose-labelled Secret. It is intentionally a plain namespaced CRD,
not a Crossplane managed resource: it has no `providerConfigRef`, connection
Secret, or management policies because it owns no external infrastructure.

## Template migration

Create a Template CR with a private, Serverless, digest-pinned image and the
supported ephemeral-disk/command/port fields. `volumeInGb` is required and must
be `0`; persistent-volume and mount-path settings are deliberately not migrated
in v0.1. RunPod defaults an omitted value to 20 GiB mounted at `/workspace`, so
the provider always sends an explicit zero. Migrate persistent storage only
after the planned `NetworkVolume` API has its own cost and deletion controls.
At least one explicit port is required; omission is not equivalent to an empty
list because RunPod would otherwise add `8888/http` and `22/tcp`.
External-name adoption is intentionally not supported in v0.1. Create a new CR
and let the provider create a new RunPod object, then retire the Terraform
object independently after validating the replacement. A user-supplied
`crossplane.io/external-name` is rejected and never queried. This avoids a typo
or transient 404 turning migration into a duplicate create, and prevents a CR
name from implicitly adopting an unrelated RunPod ID.

Status reports the provider-created external ID, observed image, Serverless
flag, and last observation time. If a durably bound RunPod object is deleted
out of band, Observe fails closed and never creates a replacement under the
immutable external-name; replace the Kubernetes CR through the reviewed GitOps
flow. Secret environment fields and registry credentials are unsupported and
never appear in status.

New external Template names receive a Kubernetes-UID suffix, just like
Endpoints. This makes post-timeout exact-name recovery specific to the CR and
prevents an ambiguous create from adopting a pre-existing same-name,
same-shape Template. The provider truncates the declared name as needed to stay
within RunPod's 191-character limit and retains the full Kubernetes UID suffix.

If a create response is ambiguous, the controller persists the exact UID-scoped
name in a recovery annotation and reports the create operation complete without
an external ID. Every later Observe performs only a list-by-owned-name lookup;
even a direct repeat of Create cannot issue another POST while the marker
exists. Exactly one matching object is adopted when it becomes visible. Zero,
multiple, or mismatched results remain fail closed. Removing that annotation is
unsupported. Because RunPod publishes no maximum visibility delay, an empty
lookup retains the marker and finalizer indefinitely. After an authorized
inventory proves absence, GitOps may set
`runpod.crossplane.io/ambiguous-create-absence-confirmed` equal to the exact
current `crossplane.io/external-create-pending` token. The live CR remains fail
closed and cannot recreate; delete it only in a later commit. A late-visible
object is still bound solely for ordered cleanup.

Updates PATCH only documented supported fields. `isServerless=true`,
`isPublic=false`, and `volumeInGb=0` are enforced. Bound Templates are always
observed with `includeEndpointBoundTemplates=true`, avoiding false 404/recreate
behavior. Deletion is idempotent; 404 is success, but direct Kubernetes and
RunPod Endpoint lists must prove that no Endpoint still references the
Template. Delete dependent Endpoint resources before the Template.

Templates have their own immutable `maxLifetimeSeconds` and independent reaper,
so a Template cannot persist forever when Endpoint admission or apply fails.
The example gives the non-compute Template a five-hour envelope (`18000`) and
the compute Endpoint/check a four-hour envelope (`14400`), leaving room for
ordered route, Endpoint, and Template cleanup. The Template reaper still refuses
deletion while any Endpoint CR or external Endpoint references it.

## Endpoint migration

`templateIdRef` is required; direct RunPod Template IDs are absent from the v0.1
API because they bypass digest, ownership, and deletion checks. The provider
resolves the referenced managed Template's controller-observed external ID. The
external endpoint name receives the full Kubernetes-UID suffix, making
ambiguous-create recovery unique even though RunPod endpoint names need not be
unique.
Endpoint creates use the same durable UID-scoped ambiguous-create recovery
marker and never repeat POST while recovery is pending.

Endpoint external-name adoption is also unsupported. Configuration must satisfy
the provider's bounds: GPU workers, one GPU per worker, `workersMin=1`,
`workersMax=1`, an idle timeout, `maxLifetimeSeconds<=86400`, an explicit
non-empty data-center allowlist, and an explicit
`maxWorkerCostMilliUsdPerHour` ceiling (the example uses `2000`, or USD 2.000
per worker-hour). The client always serializes `flashboot=false`; it never
accepts RunPod's default state-retention behavior. Status reports the external
ID, Template ID, RunPod rollout version (including valid version zero), worker
bounds, inference URL without credentials, and observation timestamp.

The continuously provisioned worker is an intentional v0.1 security tradeoff.
With `workersMin=0`, an ordinary request can create a replacement worker before
the management API exposes its cost and Secure Cloud attributes. Exactly one
worker plus the hard Endpoint TTL and hourly-cost ceiling removes that
unobservable cold-start race; any observed empty-worker interval still
withdraws routing until the replacement is directly validated.

GET requests include both Template and worker observations. Every active worker
must match the bound digest/commands/ports, zero local and network volume,
empty environment and registry auth, exact GPU/data-center allowlists, Secure
Cloud, no public IP/port mappings/savings plan, and the cost ceiling. RunPod's
official zero-volume worker shape reports `volumeEncrypted: false`; the
provider requires that field to be present but treats either boolean as inert
only after `volumeInGb: 0` is proven.

Before external deletion the client PATCHes both worker bounds to zero, then
DELETEs and observes until 404. Deletion is refused while a matching
EndpointCheck exists. At hard TTL, the janitor marks checks for deletion, waits
for model-router to cancel/drain in-flight routes, records Template cleanup
intent, and then marks the Endpoint CR for deletion. It releases its Endpoint
finalizer before Crossplane ExternalDelete (crossplane-runtime v2 refuses that
call while an extra finalizer exists). A separate Template reaper waits until
the Endpoint CR has disappeared and a direct list proves no remaining Endpoint
reference before deleting the Template CR. These operations are idempotent; a
failed route drain or external delete leaves a sequencing record for retry
instead of deleting out of order.

External Create and Update independently reject an Endpoint at or after its
creation timestamp plus `maxLifetimeSeconds`, so provider/janitor startup order
cannot create or reactivate an expired resource. An EndpointCheck using
`endpointIdRef` records the referenced Endpoint UID, Kubernetes generation, and
RunPod rollout version. Any change forces a new full model/tool verification,
and model-router direct-checks the same binding before admitting traffic.

## Management policies

The bounded `Template` and `Endpoint` CRDs both require standard Crossplane
`managementPolicies` to be exactly `['*']`. Observe-only or orphan policies are
not supported because their hard TTLs must be able to delete external objects.
EndpointCheck has no management-policy field; its hard TTL and drain finalizer
always apply.

## Deferred concepts

Pods and network volumes require separate cost, secure-cloud, resize, and leak
controls and will receive dedicated APIs later. No placeholder CRD is shipped.
Job submission, pod actions, workers, logs, billing, user lookups, and catalogs
are operations or queries rather than declarative infrastructure, so they are
not migrated to managed resources.
