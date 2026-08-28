# model-router integration

model-router exposes `GET /healthz`, `GET /readyz`, `GET /v1/models`, and
`POST /v1/chat/completions`.

`/healthz` reports process health and is the Kubernetes readiness/liveness
probe. It does not claim that an inference route exists. `/readyz` is the
client-facing route gate and returns 503 until exactly one current,
fully-verified EndpointCheck is admitted. Keeping these signals separate lets
the router Deployment become Available before the later experiment GitOps
stage creates an EndpointCheck.

The same binary runs the EndpointCheck controller. Its controller-runtime cache
is restricted to `--namespace`; referenced Endpoint and inference Secret reads
are direct, uncached API calls. Its Role has no ProviderConfig permission and
limits `get` to the exact Endpoint and Secret names used by the example.

EndpointCheck is an auxiliary plain CRD rather than a Crossplane managed
resource. It exposes only verification inputs and condition/status output;
there is no ignored management credential, connection Secret, or lifecycle
policy surface.

It advertises exactly one logical model ID: `runpod-experiment`. Configure
OpenCode with an explicit static model entry using that ID and the router Service
URL. Generic `/models` discovery is not used. The request is rewritten to the
Ready EndpointCheck `expectedModelId` and forwarded with the endpoint-scoped
token.

This single-node platform permits at most one admitted route. If two checks are
Ready, the router returns 503 until the collision is resolved. It never chooses
a winner nondeterministically.

Each forwarded request holds a lease on the admitted route. Deletion or any
other withdrawal blocks new leases, cancels upstream request contexts, and
waits for all leases to exit before acknowledging the EndpointCheck drain
finalizer. Thus a prior streaming request cannot outlive route teardown.

Ready status is accepted only for the current EndpointCheck generation, exact
referenced Endpoint UID/generation and RunPod rollout version, and exact
inference Secret version that was probed. A cheap Kubernetes
Endpoint/credential plus authenticated RunPod `/health` control-plane check
must be newer than 90 seconds; it does not submit an inference job. The
authenticated health/model/tool verification runs at admission, after relevant
generation/endpoint/credential changes, and then at the bounded
`verificationIntervalSeconds` cadence (one hour by default), not on the
30-second controller poll. Its timestamp may be no older than that interval
plus 90 seconds. Failed or timed-out full probes record their attempt timestamp
and cannot retry before the same interval; credential rotation and stale
controller status fail closed without repeatedly submitting billable inference.
For `endpointIdRef`, model-router direct-reads the Endpoint and refuses a status
bound to an older rollout before committing each route snapshot.

An initial full probe is a two-phase admission. The verifier records success but
remains Unavailable until the management controller has directly observed the
required continuously provisioned worker with bounded image, storage,
placement, Secure Cloud, exposure, and hourly-cost evidence after that probe.
Secured Endpoints require `workersMin=workersMax=1`; ordinary requests therefore
cannot race an unobserved cold placement. A management observation with an empty
worker list clears the proof and withdraws the route until the replacement is
directly observed. Missing the worker window never creates a tight probe loop:
the next full inference probe waits for `verificationIntervalSeconds`.

The referenced Secret must have
`runpod.crossplane.io/credential-purpose=inference`. Missing or opposite-purpose
labels withdraw the route even if RBAC permits the read.

Streaming chat response chunks are flushed as they arrive. Responses are
bounded to 32 MiB; because an HTTP status may already be streaming, overflow or
an upstream read failure is explicitly reported in the predeclared
`X-Provider-Runpod-Error` response trailer instead of being silently truncated.
model-router does not rewrite the upstream response model field; clients must
treat the configured logical model ID as authoritative.

Before forwarding, the router rejects client credential headers, query strings,
JSON fields named like credentials, and high-signal RunPod, GitHub, GitLab, AWS,
private-key, and JWT material. This is a fail-closed guardrail, not a general
data-loss-prevention system; callers must still classify context and run the
cluster's approved secret scanner before sending proprietary source or prompts.
The accepted request grammar is text-only and allowlisted. Tool-call argument
strings are decoded as bounded duplicate-free JSON objects and scanned again
after JSON escape decoding. Successful proxied responses always carry
`Cache-Control: no-store`, regardless of the upstream header.
