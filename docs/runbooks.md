# Operations runbook

## Create an experiment

1. Create separate restricted management and endpoint-scoped inference keys.
   Store management as `runpod-credentials/runpod-management` with
   `runpod.crossplane.io/credential-purpose=management`; store inference as
   `runpod-system/runpod-inference` with purpose `inference`.
2. Verify the model image digest, signature, SBOM, provenance, and policy.
3. Apply ProviderConfig, Template, Endpoint, and EndpointCheck through GitOps.
4. Wait for Template and Endpoint Ready, then EndpointCheck Ready.
5. Confirm model-router `/readyz` and `/v1/models` advertise
   `runpod-experiment` before enabling the OpenCode entry.

When Crossplane's RBAC manager is disabled, apply the exact provider RBAC
example before the DeploymentRuntimeConfig and Provider. Apply model-router's
namespaced Role separately; never merge the two service accounts or Secret
permissions.

## Teardown

Delete EndpointCheck first. Wait until it is gone; its router finalizer proves
the route was withdrawn. Delete Endpoint next, then Template. External 404 is a
successful terminal state. The TTL janitor performs this ordering automatically.

## Incident response

On credential exposure, revoke the key in RunPod first, remove or rotate the
Kubernetes Secret through the secret-management system, then inspect sanitized
conditions. Do not paste keys, prompts, response bodies, or Secret YAML into
issues or logs.

On route collision, list EndpointChecks in the router namespace and delete all
but the intended Ready check. The router remains fail closed throughout.

On an ambiguous create, do not manually retry POST or remove the
`*.serverless.runpod.crossplane.io/ambiguous-create-name` annotation. The
controller persists that marker and performs only exact controlled-name reads,
so eventual consistency cannot trigger a second billable create. Deleting the
CR does not turn one empty list response into proof of absence: the controller
retains its marker and finalizer indefinitely because RunPod publishes no
maximum visibility delay. If recovery remains empty, inventory RunPod through
an authorized operator session. Only after proving absence, commit
`runpod.crossplane.io/ambiguous-create-absence-confirmed` with a value exactly
equal to the CR's current `crossplane.io/external-create-pending` token. Keep the
CR present until that acknowledgement reconciles; it remains fail closed and
cannot Create. In a later GitOps commit, delete the CR. A matching token permits
finalizer removal only while the exact-name lookup remains empty; a late-visible
object is instead bound and deleted. Never force-remove the finalizer or clear
the controller recovery marker.
Transport loss, truncated or oversized response bodies, and malformed success
JSON after a POST all enter this same durable recovery state because the
provider cannot prove that RunPod did not commit the create.

## Bootstrap public release packages

GitHub creates new GHCR package records as private. Before the first release,
run the manually dispatched `Bootstrap GHCR Packages` workflow from `main`.
It tests the source, scans both architectures, and publishes unsigned candidate
versions under the exact controller, model-router, and provider package names.

An account owner must then change all three package records to **Public** in
the GitHub package settings UI. GitHub does not document a supported REST
operation for changing package visibility, so the workflows do not attempt an
unverified API mutation. Confirm that an empty Docker configuration can inspect
all three candidate references before creating `v0.1.0`.

The tag-only release workflow rechecks public metadata and anonymous pulls. It
is the only workflow with OIDC signing permission, and it verifies the exact
`release.yaml@refs/tags/<version>` identity after signing.

Before any release, enable GitHub immutable releases and configure a repository
ruleset for `refs/tags/v*.*.*` that restricts tag updates and deletions (with no
routine bypass actor), and protect `main` with the complete `CI` workflow as a
required check. GitHub does not let a workflow make its own triggering tag
immutable, so these repository settings are a manual release gate; the Actions
token cannot read the administration setting. The workflow independently requires a tag
creation event at the exact current `main` SHA, a successful `main` push CI run
for that SHA, and either absent or exact-digest-matching semver/SHA package
tags. It publishes
only run-scoped staging digests until every architecture and xpkg scan passes,
signs and attests those digests, proves all evidence anonymously, and only then
promotes the exact digests as its final registry mutation. It never rebuilds or
overwrites an existing release tag. If infrastructure failure interrupts a
partial promotion or draft publication, rerun the same GitHub workflow run.
Its complete read-only preflight accepts matching immutable destinations and
creates only missing ones; any destination containing a different digest stops
for forensic/manual review. Never delete, recreate, or overwrite final tags,
and never finish the GitHub Release manually—the workflow's evidence checks and
final immutable-publication assertions must remain the only publication path.
