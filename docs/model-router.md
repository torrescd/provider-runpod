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

It advertises exactly one logical model ID: `runpod-experiment`. Configure
OpenCode with an explicit static model entry using that ID and the router Service
URL. Generic `/models` discovery is not used. The request is rewritten to the
Ready EndpointCheck `expectedModelId` and forwarded with the endpoint-scoped
token.

This single-node platform permits at most one admitted route. If two checks are
Ready, the router returns 503 until the collision is resolved. It never chooses
a winner nondeterministically.

Ready status is accepted only for the current EndpointCheck generation, the
exact inference Secret version that was probed, and for 90 seconds after the
last successful health/model/tool verification. Credential rotation and stale
controller status fail closed.

The referenced Secret must have
`runpod.crossplane.io/credential-purpose=inference`. Missing or opposite-purpose
labels withdraw the route even if RBAC permits the read.

Streaming chat responses are passed through. model-router does not rewrite the
upstream response model field; clients must treat the configured logical model
ID as authoritative.

Before forwarding, the router rejects client credential headers, query strings,
JSON fields named like credentials, and high-signal RunPod, GitHub, GitLab, AWS,
private-key, and JWT material. This is a fail-closed guardrail, not a general
data-loss-prevention system; callers must still classify context and run the
cluster's approved secret scanner before sending proprietary source or prompts.
