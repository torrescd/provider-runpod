# Required Template image admission contract

Digest syntax validation in the provider is defense in depth; it is not proof
that the selected external workload image was built by an approved workflow.
Before any `Template` is admitted, install
`examples/policy/template-image-verification.yaml` on Kyverno 1.19 or newer and
wait for its admission webhook to be ready.

The checked-in policy intentionally denies every Template. Its keyless
`subject` is `urn:provider-runpod:unconfigured-deny-all`, which cannot be a
GitHub Actions certificate subject. A GitOps change must replace it with the
one exact reviewed model-image workflow identity, for example:

```text
issuer=https://token.actions.githubusercontent.com
subject=https://github.com/OWNER/MODEL-IMAGE-REPOSITORY/.github/workflows/release.yaml@refs/tags/v1.2.3
```

Use the exact workflow filename and tag. If multiple release tags must be
admitted, use a narrowly anchored `subjectRegExp` and review that expansion as
a policy change. Do not use an owner-, repository-, branch-, or pull-request-
wide wildcard.

The approved workflow must publish, for the exact digest placed in
`Template.spec.forProvider.imageName`:

- a Cosign keyless image signature;
- an in-toto attestation with predicate type
  `https://slsa.dev/provenance/v1`; and
- an SPDX attestation with predicate type `https://spdx.dev/Document`.

Kyverno must have registry credentials for a private image and HTTPS egress to
the image registry and Sigstore services. `failurePolicy: Fail`,
`validationActions: [Deny]`, `required: true`, and `verifyDigest: true` are part
of the security contract and must not be relaxed. Keep `mutateDigest: false`:
the Git-reviewed Template must already contain the exact digest.

Install order is strict: Kyverno CRDs and controllers, then this policy with the
approved identity, then the provider CRDs/runtime, and only then Templates.
Test both a known-good signed digest and an unsigned or wrong-identity digest
through the Kubernetes API before unsuspending the experiment stage. A registry,
Rekor, or policy-engine outage is expected to deny Template creation.
