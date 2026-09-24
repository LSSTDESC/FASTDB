#!/usr/bin/env bash
# Delete the local Kind cluster and all data stored inside it.
set -euo pipefail

CLUSTER="fastdb-local"

command -v kind >/dev/null || {
  echo "Error: kind is required." >&2
  exit 1
}

if kind get clusters | grep -Fxq "$CLUSTER"; then
  echo "Deleting Kind cluster $CLUSTER (including its local FASTDB data)..."
  kind delete cluster --name "$CLUSTER"
else
  echo "Kind cluster $CLUSTER does not exist; nothing to delete."
fi
