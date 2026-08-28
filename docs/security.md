# Security architecture

## Credential boundaries

The provider controller holds only a restricted management key needed for
Template and Endpoint CRUD. EndpointCheck reconciliation does not run in that
process; it runs beside model-router under model-router's namespace-scoped
controller manager and reads a separate endpoint-scoped inference token.

Credential Secrets require
`runpod.crossplane.io/credential-purpose=management` or `=inference`. The
provider rejects every purpose except `management`; EndpointCheck and the
router reject every purpose except `inference`. The deployment examples add a
stronger Kubernetes boundary: the provider can get only
`runpod-credentials/runpod-management`, while model-router can get only
`runpod-system/runpod-inference`. Both reads bypass controller caches.

The provider janitor retains list/delete access to EndpointChecks to enforce
route-before-endpoint teardown, but cannot update verification status or read
the inference Secret. model-router has no ProviderConfig RBAC and never holds
the management key. See [RBAC and process boundaries](rbac.md).

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
tests. All three token-bearing HTTP clients reject redirects, so an
Authorization header can never follow a 3xx response to another target.

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

Crossplane runs the provider from the selected platform manifest of the
`ghcr.io/torrescd/provider-runpod` package itself. Each platform package embeds
the corresponding provider runtime image. CI inspects the built xpkg archive to
require the expected architecture, non-root user, and provider entrypoint; the
release then scans and generates an SPDX SBOM from each complete xpkg archive
before pushing, signing, and attesting the multi-platform package digest.

The separately published `provider-runpod-controller` image is a signed mirror
for audit and direct inspection; it is not the image selected by Crossplane's
normal package runtime. The model-router remains a separate runtime image.

Release workflows scan every architecture before keyless signing and attach
signed SPDX and SLSA attestations. Every workflow action and container base is
digest/SHA pinned.
