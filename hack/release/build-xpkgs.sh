#!/usr/bin/env bash

set -euo pipefail

version="${1:?usage: build-xpkgs.sh VERSION [OUTPUT_DIR]}"
output_dir="${2:-dist/release}"

if [[ ! "${version}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "version must be a v-prefixed semantic version" >&2
  exit 1
fi

mkdir -p "${output_dir}"

source_url="${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-torrescd/provider-runpod}"
revision="${GITHUB_SHA:-unknown}"

for arch in amd64 arm64; do
  runtime_archive="${output_dir}/provider-runtime-linux-${arch}.tar"
  package_file="${output_dir}/provider-runpod-${version}-linux-${arch}.xpkg"

  docker buildx build \
    --file cluster/images/provider-runpod/Dockerfile \
    --platform "linux/${arch}" \
    --provenance=false \
    --sbom=false \
    --label "org.opencontainers.image.source=${source_url}" \
    --label "org.opencontainers.image.revision=${revision}" \
    --label "org.opencontainers.image.version=${version}" \
    --label "org.opencontainers.image.licenses=Apache-2.0" \
    --output "type=docker,dest=${runtime_archive}" \
    .

  crossplane xpkg build \
    --package-root package \
    --examples-root examples \
    --embed-runtime-image-tarball "${runtime_archive}" \
    --package-file "${package_file}"

  test -s "${runtime_archive}"
  test -s "${package_file}"
  bash hack/release/verify-xpkg-runtime.sh "${package_file}" "${arch}"
done
