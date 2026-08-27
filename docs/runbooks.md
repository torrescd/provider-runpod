# Operations runbook

## Create an experiment

1. Create separate restricted management and endpoint-scoped inference keys.
2. Verify the model image digest, signature, SBOM, provenance, and policy.
3. Apply ProviderConfig, Template, Endpoint, and EndpointCheck through GitOps.
4. Wait for Template and Endpoint Ready, then EndpointCheck Ready.
5. Confirm model-router `/readyz` and `/v1/models` advertise
   `runpod-experiment` before enabling the OpenCode entry.

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
