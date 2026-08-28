# Clean-room API sources

The implementation uses only RunPod's public REST documentation and a
hand-written subset of its published OpenAPI contract.

- REST overview and authentication: <https://docs.runpod.io/api-reference/overview>
- Templates: <https://docs.runpod.io/api-reference/templates/POST/templates>
- Bound Template reads: <https://docs.runpod.io/api-reference/templates/GET/templates/templateId>
- Endpoints: <https://docs.runpod.io/api-reference/endpoints/POST/endpoints>
- Endpoint reads: <https://docs.runpod.io/api-reference/endpoints/GET/endpoints>
- Endpoint settings and FlashBoot default: <https://docs.runpod.io/serverless/endpoints/endpoint-configurations>
- vLLM OpenAI compatibility: <https://docs.runpod.io/serverless/vllm/openai-compatibility>
- API key restrictions: <https://docs.runpod.io/get-started/api-keys>
- Public official documentation repository contract reviewed at
  `runpod/docs@f5acf42aea1e540726148e4824613dc8b2dc3b5a`,
  `api-reference/openapi.json`

The contract operations currently used are:

```text
GET,POST         /v1/templates
GET               /v1/templates/{templateId}?includeEndpointBoundTemplates=true
PATCH,DELETE      /v1/templates/{templateId}
GET,POST         /v1/endpoints
GET,PATCH,DELETE /v1/endpoints/{endpointId}
GET              /v2/{endpointId}/health
GET              /v2/{endpointId}/openai/v1/models
POST             /v2/{endpointId}/openai/v1/chat/completions
```

The raw OpenAPI file is not redistributed here. CI contract checks use
self-authored minimal fixtures and Go structs. Any future schema field must cite
an official RunPod source in its pull request.

The bounded Template contract always serializes `volumeInGb: 0` and has no
`volumeMountPath`; the official create contract otherwise defaults these to 20
and `/workspace`. It also requires and serializes explicit ports so the
documented `8888/http,22/tcp` omission default cannot appear. Endpoint placement
requires an explicit data-center allowlist. Endpoint responses are normalized from either the OpenAPI
`dataCenterIds` array or the comma-separated JSON string shown by the official
GET/POST examples. Empty segments and non-string values fail decoding.
Every Endpoint create/update serializes `flashboot:false` because RunPod
documents FlashBoot state retention as enabled by default. Endpoint status
preserves the official rollout `version`, which changes with
template/environment rollouts, for EndpointCheck readiness binding.

The RunPod Terraform provider is not an implementation or schema source for
this repository.

This release intentionally targets the official REST management API v1 at
`rest.runpod.io/v1`. RunPod documents newer management APIs as beta and has
announced an API-version transition; upgrading is tracked as a clean-room
contract change and must not be made by silently changing the compiled host or
wire schema in a patch release.
