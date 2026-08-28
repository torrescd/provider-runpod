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
`ProviderCredentials` exposes only `source: Secret` plus `secretRef`; filesystem,
environment, and injected selector fields are absent from the CRD rather than
accepted and ignored.

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
disappear. Withdrawal first blocks new leases, cancels the route context, and
waits for every in-flight upstream request to exit before removing that
finalizer. Endpoint deletion is blocked while a matching check exists.

Admission is bound to the EndpointCheck's current Kubernetes generation, the
referenced Endpoint's UID and generation, RunPod's observed Endpoint rollout
`version`, and the exact inference Secret `resourceVersion` used by the
authenticated probes. A spec edit, Endpoint recreation/rollout, resolved ID
change, or credential rotation therefore forces a new authenticated
health/model/tool verification. In steady state that potentially billable
verification runs no more often than
`verificationIntervalSeconds` (one hour by default); the 30-second reconciliation
checks Kubernetes Endpoint readiness, credential metadata, and the authenticated
RunPod `/health` control-plane endpoint without submitting an inference job.
Failed and timed-out full attempts record `lastVerificationAttemptAt` and use
the same minimum interval, so a broken model cannot create a 30-second costly
retry loop. Relevant endpoint/spec/credential changes intentionally bypass that
backoff and fail closed until newly verified. Router liveness must be newer than
90 seconds, and the last successful full verification must be
within its configured interval plus that grace, so a stopped controller cannot
leave an indefinitely stale route active without continuously invoking a GPU.

Secured v0.1 Endpoints require `workersMin=workersMax=1`. RunPod documents
`workersMin>=1` as the way to eliminate ordinary-request cold starts; allowing
zero would let a request provision a replacement before the management API
exposed its cost and Secure Cloud attributes. EndpointCheck is not routable
until a non-empty worker snapshot proves the bound image, zero storage, empty
env/auth, exact GPU/data center, Secure Cloud, no public exposure/savings plan,
and the declared `maxWorkerCostMilliUsdPerHour`. Any direct empty-worker
observation clears that proof and withdraws the route until the continuously
provisioned replacement is directly observed. The hard TTL and one-worker cost
ceiling bound the deliberate availability/security tradeoff.

Inbound Authorization and API-key headers, query parameters, RunPod-key-shaped
values, and JSON fields named like credentials are rejected. The accepted
OpenCode subset is exact and text-only; unknown vLLM extensions and media URL
parts fail before upstream traffic. Assistant tool-call arguments are separately
strict-decoded as one bounded JSON object, reject duplicate/trailing JSON, and
are recursively secret-scanned after escape decoding. Successful responses
force `Cache-Control: no-store`. Bodies are never logged and errors contain
neither response bodies nor keys.

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

The lifetime janitor holds an Endpoint finalizer only while it deletes matching
EndpointChecks and waits for router drain acknowledgement. On TTL expiry it
first records cleanup intent on the referenced Template, initiates Endpoint
deletion, then removes its Endpoint finalizer while Crossplane's managed-resource
finalizer is still present. This ordering is required by crossplane-runtime v2,
which will not call ExternalDelete while an extra finalizer remains. A separate
Template reaper waits for the Endpoint CR to disappear after external cleanup
and uses direct reads to prove no live Endpoint still references the Template
before deleting it. Manual pre-expiry Endpoint deletion is route-drained but
does not schedule automatic Template deletion. Failures leave the relevant
sequencing object terminating or marked for a later retry.

The managed Endpoint controller independently checks the CR creation deadline
immediately before every external POST or PATCH. An expired object therefore
cannot race janitor startup and create or reactivate a billable worker. Template
external deletion direct-lists all Endpoint CRs, including terminating ones,
and lists RunPod Endpoints; either Kubernetes or external reference blocks the
delete until Endpoint cleanup is proven complete.
Endpoint admission requires `managementPolicies` exactly `['*']`; observe-only
or no-delete policies are rejected because they would let the Kubernetes TTL
complete while orphaning the billed external Endpoint.
Templates require the same full lifecycle and have an independent TTL/reaper,
so permanent Endpoint admission failure cannot leave one indefinitely.

Cluster admission should reserve these controller-owned lifecycle annotations,
plus Crossplane external-create bookkeeping, to the provider identities:

- `template.serverless.runpod.crossplane.io/ambiguous-create-name`
- `template.serverless.runpod.crossplane.io/external-id-bound`
- `endpoint.serverless.runpod.crossplane.io/ambiguous-create-name`
- `endpoint.serverless.runpod.crossplane.io/external-id-bound`
- `janitor.runpod.crossplane.io/delete-when-unreferenced`
- `crossplane.io/external-name`, `crossplane.io/external-create-pending`,
  `crossplane.io/external-create-succeeded`, and
  `crossplane.io/external-create-failed`

RunPod publishes no maximum POST commit/list-visibility latency. Therefore an
empty exact-name list result never automatically releases an ambiguous-create
finalizer. After an authorized external inventory proves absence, GitOps may
set `runpod.crossplane.io/ambiguous-create-absence-confirmed` to the exact
current value of `crossplane.io/external-create-pending` while the CR remains
live. This acknowledgement keeps reconciliation fail closed and forbids Create;
a later GitOps commit may delete the CR, at which point the matching token alone
permits finalizer removal if the exact-name lookup is still empty. Cluster
admission must reject this acknowledgement on object creation and restrict all
changes to the audited GitOps identity; the provider identity must not write it.

## Image policy

Every Template must declare `volumeInGb: 0`. The CRD rejects any non-zero value
and requires the field to be present; the management client serializes the zero
without `omitempty`. `volumeMountPath` is not part of the provider API and is
never sent. These invariants prevent RunPod from applying its omitted-field
defaults of a 20 GiB persistent volume mounted at `/workspace`.
Templates must also declare at least one validated `port/protocol` entry, so
RunPod cannot silently apply its omitted-field defaults of `8888/http` and
`22/tcp`. Platform admission should restrict this explicit list to the model's
reviewed listener (the example uses only `8000/http`). Endpoints likewise
require a non-empty, explicit data-center allowlist instead of broad default
placement. Endpoint create/update also always serializes `flashboot: false`;
RunPod otherwise enables worker-state retention by default. The published
Endpoint response does not consistently expose FlashBoot, so the provider can
detect a `true` value only when it is returned. Platform policy must restrict
console/API mutation, and live activation must confirm FlashBoot remains off.

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
