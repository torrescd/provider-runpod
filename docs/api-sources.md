# Clean-room API sources

The implementation uses only RunPod's public REST documentation and a
hand-written subset of its published OpenAPI contract.

- REST overview and authentication: <https://docs.runpod.io/api-reference/overview>
- Templates: <https://docs.runpod.io/api-reference/templates/POST/templates>
- Endpoints: <https://docs.runpod.io/api-reference/endpoints/POST/endpoints>
- vLLM OpenAI compatibility: <https://docs.runpod.io/serverless/vllm/openai-compatibility>
- API key restrictions: <https://docs.runpod.io/get-started/api-keys>
- Public official documentation repository contract reviewed at
  `runpod/docs@f5acf42aea1e540726148e4824613dc8b2dc3b5a`,
  `api-reference/openapi.json`

The contract operations currently used are:

```text
GET,POST         /v1/templates
GET,PATCH,DELETE /v1/templates/{templateId}
GET,POST         /v1/endpoints
GET,PATCH,DELETE /v1/endpoints/{endpointId}
GET              /v2/{endpointId}/health
GET              /v2/{endpointId}/openai/v1/models
POST             /v2/{endpointId}/openai/v1/chat/completions
```

The raw OpenAPI file is not redistributed here. CI contract checks use
self-authored minimal fixtures and Go structs. Any future schema field must cite
an official RunPod source in its pull request.

The RunPod Terraform provider is not an implementation or schema source for
this repository.
