# model-router integration

model-router exposes `GET /healthz`, `GET /readyz`, `GET /v1/models`, and
`POST /v1/chat/completions`.

It advertises exactly one logical model ID: `runpod-experiment`. Configure
OpenCode with an explicit static model entry using that ID and the router Service
URL. Generic `/models` discovery is not used. The request is rewritten to the
Ready EndpointCheck `expectedModelId` and forwarded with the endpoint-scoped
token.

This single-node platform permits at most one admitted route. If two checks are
Ready, the router returns 503 until the collision is resolved. It never chooses
a winner nondeterministically.

Streaming chat responses are passed through. model-router does not rewrite the
upstream response model field; clients must treat the configured logical model
ID as authoritative.
