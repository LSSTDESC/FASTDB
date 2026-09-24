#!/usr/bin/env bash
# Build and install FASTDB on the existing local Kind cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="fastdb-local"
KUBE_CONTEXT="kind-$CLUSTER_NAME"
NAMESPACE="fastdb-local"
RELEASE_NAME="fastdb"
VALUES_FILE="$REPO_ROOT/helm/fastdb/values-local.yaml"
EXTERNAL_URL="http://localhost:8080/"

DOCKER_ARCHIVE="${DOCKER_ARCHIVE:-ghcr.io/lsstdesc}"
DOCKER_VERSION="${DOCKER_VERSION:-test20260428}"
BUSYBOX_IMAGE="docker.io/library/busybox:1.36.1"
MAILHOG_IMAGE="docker.io/mailhog/mailhog:v1.0.1"

for command_name in docker helm kind kubectl; do
  command -v "$command_name" >/dev/null || {
    echo "Error: $command_name is required." >&2
    exit 1
  }
done

docker info >/dev/null 2>&1 || {
  echo "Error: Docker is not running." >&2
  exit 1
}

if ! kind get clusters | grep -Fxq "$CLUSTER_NAME"; then
  echo "Error: Kind cluster '$CLUSTER_NAME' does not exist." >&2
  echo "Create it first with ./helm/scripts/create-local-cluster.sh" >&2
  exit 1
fi

cd "$REPO_ROOT"

echo "Building FASTDB container images..."
# The createdb service builds the shared fastdb-shell image used by createdb,
# shell, and the install builder. Build it exactly once.
docker compose build postgres mongodb createdb webap queryrunner

echo "Building FASTDB install/ for $EXTERNAL_URL..."
docker compose run --rm --entrypoint "" makeinstall /bin/bash -ec "
  touch aclocal.m4 configure
  find . -name Makefile.am -exec touch {} \\;
  find . -name Makefile.in -exec touch {} \\;
  ./configure \\
    --with-installdir=/fastdb \\
    --with-smtp-server=mailhog \\
    --with-smtp-port=1025 \\
    --with-external-url=$EXTERNAL_URL
  make install
"

echo "Making external images available locally..."
for image_name in "$BUSYBOX_IMAGE" "$MAILHOG_IMAGE"; do
  if ! docker image inspect "$image_name" >/dev/null 2>&1; then
    docker pull "$image_name"
  fi
done

echo "Loading images into Kind..."
for image_name in \
  "$DOCKER_ARCHIVE/fastdb-postgres:$DOCKER_VERSION" \
  "$DOCKER_ARCHIVE/fastdb-mongodb:$DOCKER_VERSION" \
  "$DOCKER_ARCHIVE/fastdb-shell:$DOCKER_VERSION" \
  "$DOCKER_ARCHIVE/fastdb-webap:$DOCKER_VERSION" \
  "$DOCKER_ARCHIVE/fastdb-query-runner:$DOCKER_VERSION"; do
  echo "  $image_name"
  kind load docker-image --name "$CLUSTER_NAME" "$image_name"
done

# Docker Desktop stores pulled multi-platform images differently from locally
# built images. Import only the downloaded platform into Kind's containerd;
# asking for every platform fails because the other platforms are not local.
for image_name in "$BUSYBOX_IMAGE" "$MAILHOG_IMAGE"; do
  echo "  $image_name"
  image_archive="$(mktemp "/tmp/fastdb-kind-image.XXXXXX.tar")"
  docker image save --output "$image_archive" "$image_name"
  docker exec --privileged -i "$CLUSTER_NAME-control-plane" \
    ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs - \
    < "$image_archive"
  rm -f "$image_archive"
done

echo "Installing FASTDB with Helm..."
# A Helm Job has an immutable pod template. Remove an earlier migration Job so
# every installation runs the current migrations and Helm can recreate it.
kubectl --context "$KUBE_CONTEXT" delete job createdb \
  --namespace "$NAMESPACE" --ignore-not-found

if ! helm upgrade --install "$RELEASE_NAME" "$REPO_ROOT/helm/fastdb" \
  --kube-context "$KUBE_CONTEXT" \
  --namespace "$NAMESPACE" \
  --create-namespace \
  --values "$VALUES_FILE" \
  --wait \
  --wait-for-jobs \
  --timeout 5m; then
  echo "Helm installation failed. Current pod status:" >&2
  kubectl --context "$KUBE_CONTEXT" get pods --namespace "$NAMESPACE" || true
  kubectl --context "$KUBE_CONTEXT" logs \
    --namespace "$NAMESPACE" job/createdb || true
  exit 1
fi

echo "Checking the migration Job..."
if ! kubectl --context "$KUBE_CONTEXT" wait \
  --namespace "$NAMESPACE" \
  --for=condition=complete job/createdb \
  --timeout=30s; then
  kubectl --context "$KUBE_CONTEXT" logs \
    --namespace "$NAMESPACE" job/createdb || true
  exit 1
fi

echo "Restarting services that load FASTDB Python code..."
for deployment_name in webap queryrunner brokerconsumer; do
  if kubectl --context "$KUBE_CONTEXT" get deployment "$deployment_name" \
    --namespace "$NAMESPACE" >/dev/null 2>&1; then
    echo "  $deployment_name"
    kubectl --context "$KUBE_CONTEXT" rollout restart \
      "deployment/$deployment_name" --namespace "$NAMESPACE"
    kubectl --context "$KUBE_CONTEXT" rollout status \
      "deployment/$deployment_name" --namespace "$NAMESPACE" --timeout=120s
  fi
done

echo
kubectl --context "$KUBE_CONTEXT" get pods --namespace "$NAMESPACE"
echo
echo "FASTDB is ready:  http://localhost:8080"
echo "MailHog is ready: http://localhost:8025"
