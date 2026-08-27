# Security policy

Report vulnerabilities privately through GitHub Security Advisories for
`torrescd/provider-runpod`. Do not open a public issue containing credentials,
request payloads, model prompts, or vulnerability details.

Supported releases are the latest signed minor release and the current `main`.
Release artifacts must be verified against the keyless identity documented in
the release notes before deployment.

RunPod management and inference keys must be restricted, separate, rotated, and
stored only in Kubernetes Secrets outside Git. A legacy unrestricted account
key is outside the supported threat model.
