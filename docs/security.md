# Security architecture

## Credential boundaries

The provider controller holds only a restricted management key needed for
Template and Endpoint CRUD. EndpointCheck reads a separate endpoint-scoped
inference token. model-router has RBAC for EndpointChecks and `get` on Secrets,
but has no access to ProviderConfig resources and never holds the management
key.

OpenCode talks only to model-router. It sends model ID `runpod-experiment` and
stores no RunPod token. The router rewrites that ID to the currently admitted
EndpointCheck `expectedModelId`.

## Fail-closed routing

Exactly one Ready, non-deleting, unexpired EndpointCheck may be active in the
configured namespace. Zero checks, multiple checks, stale status, a missing
Secret, or a Kubernetes API error withdraws the route. A router-owned finalizer
acknowledges that the route has been withdrawn before an EndpointCheck can
disappear. Endpoint deletion is blocked while a matching check exists.

Admission is bound to the EndpointCheck's current Kubernetes generation and to
the exact inference Secret `resourceVersion` used by the authenticated probes.
A spec edit or credential rotation therefore withdraws the route until all
checks pass again. Verification status older than 90 seconds is rejected, so a
stopped check controller cannot leave an indefinitely stale route active.

Inbound Authorization and API-key headers, query parameters, RunPod-key-shaped
values, and JSON fields named like credentials are rejected. Bodies are never
logged and errors contain neither response bodies nor keys.

## Runtime

Both images run as UID/GID 65532 with a read-only root filesystem, no privilege
escalation, no Linux capabilities, and RuntimeDefault seccomp. The provider and
router each honor `HTTPS_PROXY`. Cluster policy should deny direct public egress
and allow only their respective CONNECT proxies:

- provider: `rest.runpod.io:443`
- model-router: `api.runpod.ai:443`

The production base URLs are compiled in. Only a loopback override exists for
tests.

The lifetime janitor holds its own Endpoint finalizer. It deletes matching
EndpointChecks and waits for router drain acknowledgement, initiates Endpoint
deletion, waits for Crossplane's external-resource finalizer to disappear, and
only then deletes an unshared referenced Template. Manual Endpoint deletion is
also route-drained. If router acknowledgement or external deletion fails, the
objects remain terminating instead of being leaked or deleted out of order.

## Image policy

Template creation rejects mutable tags. Admission must additionally verify the
digest's Cosign identity, SPDX SBOM, provenance, and vulnerability policy.
If verification is unavailable, RunPod resource creation must fail closed.

Release workflows scan images before keyless signing and attach signed SPDX and
SLSA attestations. Every workflow action and container base is digest/SHA pinned.
