#!/usr/bin/env bash
#
# Deploy FASTDB to Kubernetes via Helm.
#
# Builds install/, runs helm upgrade --install, copies code to the PVC
# via the shell pod, and restarts pods that depend on the code.
#
# Usage:
#   ./scripts/helm-deploy.sh [NAMESPACE] [VALUES_FILE] [OPTIONS]
#
# Arguments:
#   NAMESPACE    Kubernetes namespace (default: local)
#   VALUES_FILE  Helm values file   (default: ./helm/fastdb/values-local.yaml)
#
# Options:
#   --skip-build             Skip the docker-compose makeinstall step
#   --skip-helm              Skip helm upgrade --install (just copy code + restart)
#   --release NAME           Helm release name (default: fastdb)
#   --create-cluster FILE    Create a Kind cluster before deploying. FILE is the Kind config
#                            template (e.g., admin/local/kind-config.yaml). ${PWD} in hostPath
#                            entries is expanded to the current directory. Cluster name is
#                            set to NAMESPACE and --context is set to kind-NAMESPACE.
#   --context NAME           Kubernetes context to use (default: current kubeconfig context)
#   --registry-password PAT  Registry password/token (passed to Helm as registryCredentials.password)
#   --external-url PATH      Base path for subdirectory deployments (e.g., /fastdb-ccosta-dev/)
#                            Must match webap.basePath in your values file, with a trailing slash.
#                            Bakes the path into frontend JS/HTML during the build step.
#   --load-images            Build container images and load them into the Kind cluster.
#                            Implied by --create-cluster. Uses DOCKER_ARCHIVE and DOCKER_VERSION
#                            env vars (defaults: ghcr.io/lsstdesc, test20251201).
#   -h, --help               Show this help message
#

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────────────
NS="${1:-local}"
VALUES="${2:-./helm/fastdb/values-local.yaml}"
RELEASE="fastdb"
SKIP_BUILD=false
SKIP_HELM=false
REGISTRY_PASSWORD=""
EXTERNAL_URL="/"
KUBE_CONTEXT=""
CREATE_CLUSTER=""
LOAD_IMAGES=false
NAMESPACE=${1:-fastdb-local}
CLUSTER_NAME=${CLUSTER_NAME:-$NAMESPACE}

# ── Parse optional flags (after positional args) ─────────────────────
shift 2 2>/dev/null || true
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)          SKIP_BUILD=true;       shift ;;
    --skip-helm)           SKIP_HELM=true;        shift ;;
    --release)             RELEASE="$2";          shift 2 ;;
    --registry-password)   REGISTRY_PASSWORD="$2"; shift 2 ;;
    --external-url)        EXTERNAL_URL="$2";      shift 2 ;;
    --context)             KUBE_CONTEXT="$2";      shift 2 ;;
    --create-cluster)      CREATE_CLUSTER="$2";    shift 2 ;;
    --load-images)         LOAD_IMAGES=true;       shift ;;
    -h|--help)
      sed -n '2,/^$/s/^# \?//p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
done

# ── Create Kind cluster (if requested) ─────────────────────────────
if [[ -n "$CREATE_CLUSTER" ]]; then
  if ! command -v kind &>/dev/null; then
    echo "Error: kind is required for --create-cluster but not found in PATH" >&2
    exit 1
  fi
  if [[ ! -f "$CREATE_CLUSTER" ]]; then
    echo "Error: Kind config not found: $CREATE_CLUSTER" >&2
    exit 1
  fi
  if kind get clusters 2>/dev/null | grep -qx "$NS"; then
    echo "Kind cluster '$NS' already exists, skipping creation."
  else
    echo "--- Creating Kind cluster '$NS' from $CREATE_CLUSTER ---"
    sed "s|\${PWD}|$PWD|g" "$CREATE_CLUSTER" | kind create cluster --name "$NS" --config -
  fi
  KUBE_CONTEXT="kind-$NS"
  LOAD_IMAGES=true
  echo ""
fi

# Create directories
docker exec "$CLUSTER_NAME"-control-plane mkdir -p /fastdb-install
docker exec "$CLUSTER_NAME"-control-plane mkdir -p /fastdb-db

# ── Preflight checks ────────────────────────────────────────────────
for cmd in kubectl helm; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: $cmd is required but not found in PATH" >&2
    exit 1
  fi
done

# Detect container runtime: prefer podman, fall back to docker
if command -v podman &>/dev/null; then
  CONTAINER_RT=podman
elif command -v docker &>/dev/null; then
  CONTAINER_RT=docker
else
  echo "Error: podman or docker is required but neither was found in PATH" >&2
  exit 1
fi

if [[ ! -f "$VALUES" ]]; then
  echo "Error: values file not found: $VALUES" >&2
  exit 1
fi

# Build context args for kubectl and helm
KUBECTL_CTX=()
HELM_CTX=()
if [[ -n "$KUBE_CONTEXT" ]]; then
  KUBECTL_CTX=(--context "$KUBE_CONTEXT")
  HELM_CTX=(--kube-context "$KUBE_CONTEXT")
fi

echo "=== FASTDB Helm Deploy ==="
echo "  Namespace    : $NS"
echo "  Values       : $VALUES"
echo "  Release      : $RELEASE"
echo "  Runtime      : $CONTAINER_RT"
echo "  Context      : ${KUBE_CONTEXT:-$(kubectl config current-context 2>/dev/null || echo '(unknown)')}"
if [[ -n "$EXTERNAL_URL" ]]; then
  echo "  External URL : $EXTERNAL_URL"
fi
echo ""

# ── Step 1: Build install/ ───────────────────────────────────────────
if [[ "$SKIP_BUILD" == "false" ]]; then
  if [[ -n "$EXTERNAL_URL" ]]; then
    echo "--- Building install/ with --with-external-url=$EXTERNAL_URL ---"
    $CONTAINER_RT compose run --rm --entrypoint "" makeinstall /bin/bash -c "
      touch aclocal.m4 configure \
      && find . -name Makefile.am -exec touch {} \; \
      && find . -name Makefile.in -exec touch {} \; \
      && ./configure \
           --with-installdir=/fastdb \
           --with-smtp-server=mailhog \
           --with-smtp-port=1025 \
           --with-external-url=$EXTERNAL_URL \
      && make install
    "
  else
    echo "--- Building install/ via docker-compose makeinstall ---"
    $CONTAINER_RT compose run --rm makeinstall
  fi
  echo ""
fi

# Verify install/ exists and has content
if [[ ! -d install ]] || [[ -z "$(ls -A install 2>/dev/null)" ]]; then
  echo "Error: install/ directory is missing or empty." >&2
  echo "Run 'docker-compose run --rm makeinstall' first, or use --skip-build if already built." >&2
  exit 1
fi

# ── Step 1b: Build and load images into Kind ──────────────────────────
if [[ "$LOAD_IMAGES" == "true" ]]; then
  DOCKER_ARCHIVE="${DOCKER_ARCHIVE:-ghcr.io/lsstdesc}"
  DOCKER_VERSION="${DOCKER_VERSION:-test20251201}"

  echo "--- Building container images ---"
  $CONTAINER_RT compose build postgres postgres-standby mongodb shell webap queryrunner
  echo ""

  echo "--- Loading images into Kind cluster '$NS' ---"
  # Save and load each image individually. podman save does not correctly
  # preserve multiple images in a single archive (all tags collapse to one
  # image), so we must handle them one at a time.
  for img in postgres postgres-standby mongodb shell webap query-runner; do
    local_tag="${DOCKER_ARCHIVE}/fastdb-${img}:${DOCKER_VERSION}"
    echo "  Loading $local_tag"
    IMAGE_TAR=$(mktemp /tmp/fastdb-kind-${img}.XXXXXX.tar)
    $CONTAINER_RT save -o "$IMAGE_TAR" "$local_tag"
    kind load image-archive "$IMAGE_TAR" --name "$NS"
    rm -f "$IMAGE_TAR"
  done
  echo ""
fi


# ── Step 1c: Install certs into Kind ───────────────────────────────────
echo "--- Setting up MinIO certs ---"
./scripts/setup-kind-certs.sh "$NS"
echo ""

# ── Step 2: Helm upgrade --install ───────────────────────────────────
if [[ "$SKIP_HELM" == "false" ]]; then
  echo "--- Running helm upgrade --install ---"
  HELM_SET_ARGS=()
  if [[ -n "$REGISTRY_PASSWORD" ]]; then
    HELM_SET_ARGS+=(--set "global.registryCredentials.password=$REGISTRY_PASSWORD")
  fi
  helm upgrade --install "$RELEASE" ./helm/fastdb -f "$VALUES"\
    --create-namespace -n "$NS" \
    "${HELM_CTX[@]+"${HELM_CTX[@]}"}" \
    "${HELM_SET_ARGS[@]+"${HELM_SET_ARGS[@]}"}"
  echo ""
fi

# ── Step 3: Wait for shell pod ───────────────────────────────────────
echo "--- Waiting for shell pod to be ready ---"
kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" wait --for=condition=available \
  deployment/shell -n "$NS" --timeout=120s

SHELL_POD=$(kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" get pods -n "$NS" \
  -l app=shell -o jsonpath='{.items[0].metadata.name}')
echo "  Shell pod: $SHELL_POD"
echo ""

# ── Step 4: Copy code to PVC ────────────────────────────────────────
echo "--- Copying install/ contents to /fastdb/ on PVC ---"
COPYFILE_DISABLE=1 tar cf - -C install . \
  | kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" exec -i -n "$NS" "$SHELL_POD" \
    -- tar xf - -C /fastdb/ 2>/dev/null
echo "  install/ copied."

echo "--- Copying db/ contents to /fastdb/db/ on PVC ---"
kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" exec -n "$NS" "$SHELL_POD" \
  -- mkdir -p /fastdb/db
COPYFILE_DISABLE=1 tar cf - -C db . \
  | kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" exec -i -n "$NS" "$SHELL_POD" \
    -- tar xf - -C /fastdb/db/ 2>/dev/null
echo "  db/ copied."
echo ""

# ── Step 5: Restart pods that depend on the code ─────────────────────
echo "--- Restarting webap and queryrunner ---"
kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" rollout restart deployment/webap \
  -n "$NS" 2>/dev/null \
  && kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" rollout status deployment/webap \
    -n "$NS" --timeout=120s \
  || echo "  (webap deployment not found or not enabled, skipping)"

kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" rollout restart deployment/queryrunner \
  -n "$NS" 2>/dev/null \
  && kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" rollout status deployment/queryrunner \
    -n "$NS" --timeout=120s \
  || echo "  (queryrunner deployment not found or not enabled, skipping)"
echo ""

# ── Step 6: Status ───────────────────────────────────────────────────
echo "--- Pod status ---"
kubectl "${KUBECTL_CTX[@]+"${KUBECTL_CTX[@]}"}" get pods -n "$NS"
echo ""
echo "=== Deploy complete ==="
