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
checks before routing.

## Template migration and adoption

Create a Template CR with a private, Serverless, digest-pinned image and the
supported disk/command/port fields. To adopt an existing RunPod Template, set
the standard annotation before creation:

```yaml
metadata:
  annotations:
    crossplane.io/external-name: existing-runpod-template-id
```

The first Observe reads the object by ID. A 404 means absent. Status reports the
external ID, observed image, Serverless flag, and last observation time. Secret
environment fields and registry credentials are not supported in v0.1 and
never appear in status.

Updates PATCH only documented supported fields. `isServerless=true` and
`isPublic=false` are enforced. Deletion is idempotent; 404 is success. Delete
dependent Endpoint resources before the Template. When an Endpoint uses
`templateIdRef`, the TTL janitor can delete that referenced Template after route
withdrawal.

## Endpoint migration and adoption

Prefer `templateIdRef` to a direct RunPod ID. The provider resolves the
referenced Template's external name. The external endpoint name receives a
Kubernetes-UID suffix, making ambiguous-create recovery unique even though
RunPod endpoint names need not be unique.

Adopt an existing endpoint with `crossplane.io/external-name`. The declared
configuration must satisfy the provider's bounds: GPU workers, one GPU per
worker, `workersMin=0`, `workersMax<=1`, an idle timeout, and
`maxLifetimeSeconds<=86400`. Status reports the external ID, Template ID, worker
bounds, inference URL without credentials, and observation timestamp.

Before external deletion the client PATCHes both worker bounds to zero, then
DELETEs and observes until 404. Deletion is refused while a matching
EndpointCheck exists. At hard TTL, the janitor marks checks for deletion, waits
for model-router to acknowledge route withdrawal, then deletes the Endpoint CR
idempotently and optionally its referenced Template CR.

## Management policies

Standard Crossplane management policies are supported. For adoption without
mutation, begin with `managementPolicies: [Observe]`; after validating status,
explicitly enable desired actions. Platform admission should forbid
non-deleting experiment policies where leak-free cleanup is required.

## Deferred concepts

Pods and network volumes require separate cost, secure-cloud, resize, and leak
controls and will receive dedicated APIs later. No placeholder CRD is shipped.
Job submission, pod actions, workers, logs, billing, user lookups, and catalogs
are operations or queries rather than declarative infrastructure, so they are
not migrated to managed resources.
