# provider-runpod

`provider-runpod` is a clean-room, Apache-2.0 Crossplane v2 provider for the
official RunPod REST API. It currently manages private, digest-pinned Serverless
`Template` and cost-bounded `Endpoint` resources. `EndpointCheck` admits exactly
one endpoint to the optional model router only after authenticated health,
model identity, and tool-call checks pass.

This repository was initialized from `crossplane/provider-template` commit
`8bb059faca2a9b6340aec0285265e493107df603`. No RunPod Terraform provider
source, schema, generated code, or fixtures are used.

## Supported resources

| API | Purpose |
|---|---|
| `Template.serverless.runpod.crossplane.io/v1alpha1` | Private Serverless template using an OCI digest |
| `Endpoint.serverless.runpod.crossplane.io/v1alpha1` | GPU endpoint with `workersMin=0`, `workersMax<=1`, and hard TTL |
| `EndpointCheck.verification.runpod.crossplane.io/v1alpha1` | Inference-only readiness and routing gate |

Pods and network volumes are deliberately deferred. Imperative actions,
inference jobs, workers, billing, logs, and Terraform-style data sources are not
implemented.

## Security model

- The production management URL is fixed at `https://rest.runpod.io/v1`.
- The production inference URL is fixed at `https://api.runpod.ai`.
- Both clients honor `HTTP_PROXY`/`HTTPS_PROXY`; only loopback URLs may be
  substituted in tests.
- A namespaced ProviderConfig can only use a management Secret in its namespace.
- EndpointCheck uses a distinct local, endpoint-scoped inference Secret and
  rejects a Secret referenced by any ProviderConfig.
- model-router has no ProviderConfig RBAC and never holds the management key.
- Template images must be `repository@sha256:<64 lowercase hex>` and should also
  be verified by admission policy before resource creation.
- Endpoint and route lifetimes are hard-bounded to 24 hours.

See [security](docs/security.md), [API sources](docs/api-sources.md), the
[Terraform migration guide](docs/terraform-migration.md), and the
[operations runbook](docs/runbooks.md).

## Development

```sh
git submodule update --init --recursive
make reviewable
make build
```

Unit and fake-controller tests never call RunPod. Live acceptance is manual,
uses a newly created restricted key, and is intentionally absent from ordinary
pull request CI.
