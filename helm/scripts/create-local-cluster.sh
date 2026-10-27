#!/usr/bin/env bash
# Create the Kind cluster used for local FASTDB development.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER_NAME="fastdb-local"
KUBE_CONTEXT="kind-$CLUSTER_NAME"
KIND_CONFIG="$REPO_ROOT/admin/local/kind-config.yaml"

for command_name in docker kind kubectl sed; do
  command -v "$command_name" >/dev/null || {
    echo "Error: $command_name is required." >&2
    exit 1
  }
done

docker info >/dev/null 2>&1 || {
  echo "Error: Docker is not running." >&2
  exit 1
}

if kind get clusters | grep -Fxq "$CLUSTER_NAME"; then
  echo "Kind cluster '$CLUSTER_NAME' already exists."
else
  echo "Creating Kind cluster '$CLUSTER_NAME'..."
  sed "s|\${PWD}|$REPO_ROOT|g" "$KIND_CONFIG" |
    kind create cluster --name "$CLUSTER_NAME" --config -
fi

echo "Checking cluster access..."
kubectl --context "$KUBE_CONTEXT" cluster-info

echo
echo "Local Kind cluster is ready."
echo "Context: $KUBE_CONTEXT"
