#!/usr/bin/env bash

set -euo pipefail

package_file="${1:?usage: verify-xpkg-runtime.sh XPKG ARCH}"
expected_arch="${2:?usage: verify-xpkg-runtime.sh XPKG ARCH}"

test -s "${package_file}"
manifest="$(tar -xOf "${package_file}" manifest.json)"
config_file="$(jq -er '
  if length == 1 and .[0].Config != null then .[0].Config
  else error("xpkg must contain exactly one Docker archive manifest")
  end' <<<"${manifest}")"

tar -xOf "${package_file}" "${config_file}" |
  jq -e --arg arch "${expected_arch}" '
    .os == "linux" and
    .architecture == $arch and
    .config.User == "65532:65532" and
    .config.Entrypoint == ["/usr/local/bin/crossplane-runpod-provider"]' >/dev/null
