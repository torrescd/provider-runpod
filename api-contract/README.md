# API contract policy

Fixtures in this directory, when present, are minimal responses authored for
provider tests from the operations listed in `docs/api-sources.md`. They must
not be copied from SDKs, Terraform providers, captured customer traffic, or
authenticated production responses.

An authenticated OpenAPI drift job may fetch
`https://rest.runpod.io/v1/openapi.json` ephemerally. The key and downloaded
contract must not be persisted as an artifact or committed unless RunPod
explicitly permits redistribution.
