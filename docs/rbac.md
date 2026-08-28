# RBAC and process boundaries

provider-runpod uses two service accounts and two operating-system processes.
The provider process reconciles `Template` and `Endpoint` and may read only a
management-purpose Secret. model-router reconciles `EndpointCheck` and may read
only an inference-purpose Secret. Neither process accepts a Secret unless it
has the exact label below:

| Consumer | Required label |
|---|---|
| provider | `runpod.crossplane.io/credential-purpose=management` |
| model-router and EndpointCheck controller | `runpod.crossplane.io/credential-purpose=inference` |

Missing, misspelled, or opposite-purpose labels fail closed. The label is a
second boundary in addition to RBAC; do not grant either service account
permission to mutate Secret labels.

## Crossplane with the RBAC manager disabled

Crossplane's `rbacManager.deploy=false` Helm value requires operators to supply
provider permissions. Apply
`examples/rbac/provider-rbac-manager-disabled.yaml` before installing the
provider, then use `examples/runtime/deployment-runtime-config.yaml` so the
package runtime adopts the exact `crossplane-system/provider-runpod` service
account.

The provider ClusterRole deliberately has no Secret rule. A separate Role in
`runpod-credentials` grants `get` on only `runpod-management` to the
`crossplane-system/provider-runpod` service account, as referenced by the
example ClusterProviderConfig. The provider janitor can list and delete
EndpointChecks so it can withdraw a route before deleting an Endpoint; this
reveals only check metadata and references, not the inference token. It has no
permission in `runpod-system` to read `runpod-inference`, and cannot update
EndpointCheck readiness.

The examples intentionally keep runtime and both credential domains in separate
namespaces: the provider service account in `crossplane-system`, management in
`runpod-credentials`, and inference in `runpod-system`. If a
namespaced ProviderConfig is used instead, its management Secret must be in the
managed resource's namespace and an equally narrow Secret Role must be added
there; do not grant cluster-wide Secret access.

The example assumes leader election remains disabled, which is the provider
default. If an operator enables it, add a namespace-scoped Role for the named
Lease rather than widening the shipped ClusterRole.

## model-router

`examples/router/model-router.yaml` grants only:

- get/list/watch/update/delete on EndpointChecks, for reconciliation,
  self-expiry, and drain-finalizer acknowledgement;
- update on EndpointCheck status;
- get on the one referenced Endpoint name; and
- get on the one inference Secret name.

The Role is namespaced and has no ProviderConfig or management-Secret access.
Keep both `resourceNames` entries synchronized with the EndpointCheck example
if names are changed. model-router's controller-runtime cache is restricted to
its configured namespace; Secret and Endpoint reads bypass the cache and go
directly to the API server.
