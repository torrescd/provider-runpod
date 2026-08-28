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

On an ambiguous create, do not manually retry POST. The controller performs an
exact controlled-name recovery. If recovery is ambiguous, inventory RunPod
through an authorized operator session and remove leaks before resuming.

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
