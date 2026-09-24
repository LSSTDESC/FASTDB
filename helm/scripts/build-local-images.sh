#!/usr/bin/env bash
# Build FASTDB images from this checkout and load them into local K3s.
set -euo pipefail

####################
##### Preamble
####################

# Locate the repository and accept the desired registry and tag as a pair.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
case $# in
  0)
    DEFAULT_IMAGE_REGISTRY="fastdb.local"
    DEFAULT_IMAGE_TAG="test20260428"
    echo "No image registry or tag was provided."
    echo "Suggested defaults:"
    echo "  Image registry: $DEFAULT_IMAGE_REGISTRY"
    echo "  Image tag:      $DEFAULT_IMAGE_TAG"
    read -r -p "Use these defaults? [Y/n] " defaults_reply
    case "$defaults_reply" in
      ""|y|Y|yes|YES|Yes)
        export DOCKER_ARCHIVE="$DEFAULT_IMAGE_REGISTRY"
        export DOCKER_VERSION="$DEFAULT_IMAGE_TAG"
        ;;
      n|N|no|NO|No)
        echo "Rerun with both the desired registry and tag:"
        echo "  $0 <image-registry> <image-tag>"
        exit 0
        ;;
      *)
        echo "Error: please answer y or n." >&2
        exit 1
        ;;
    esac
    ;;
  1)
    echo "Error: image registry and image tag must be provided together." >&2
    echo "Usage: $0 [<image-registry> <image-tag>]" >&2
    exit 1
    ;;
  2)
    export DOCKER_ARCHIVE="$1"
    export DOCKER_VERSION="$2"
    ;;
  *)
    echo "Usage: $0 [<image-registry> <image-tag>]" >&2
    echo "Example: $0 fastdb.local test20260428" >&2
    exit 1
    ;;
esac

# Require a normal user with access to Docker, K3s, Git, and sudo.
if [[ $EUID -eq 0 ]]; then
  echo "Error: run this script as your normal user, not with sudo." >&2
  exit 1
fi
for command_name in docker git kubectl sudo; do
  command -v "$command_name" >/dev/null || {
    echo "Error: $command_name is required." >&2
    exit 1
  }
done
docker info >/dev/null 2>&1 || {
  echo "Error: cannot access Docker as $(id -un)." >&2
  exit 1
}
kubectl get node >/dev/null || {
  echo "Error: the K3s cluster is not accessible through kubectl." >&2
  exit 1
}

# Initialize build inputs before starting the long image builds.
echo "Initializing Git submodules..."
git -C "$REPO_ROOT" submodule update --init --recursive

# Authenticate once and keep sudo active until all images are loaded into K3s.
echo "Administrator access will be needed to load images into K3s."
sudo -v
SUDO_KEEPALIVE_PID=""
cleanup() {
  if [[ -n "$SUDO_KEEPALIVE_PID" ]]; then
    kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
    wait "$SUDO_KEEPALIVE_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT
(
  while true; do
    sudo -n -v || exit
    sleep 60
  done
) &
SUDO_KEEPALIVE_PID=$!

echo "Building images as $DOCKER_ARCHIVE/fastdb-*:$DOCKER_VERSION"

####################
##### End of preamble
####################

cd "$REPO_ROOT"

# Build every FASTDB runtime image sequentially to limit peak disk and memory use.
services=(postgres mongodb createdb webap queryrunner)
repositories=(
  fastdb-postgres
  fastdb-mongodb
  fastdb-shell
  fastdb-webap
  fastdb-query-runner
)
for index in "${!services[@]}"; do
  echo "Building ${repositories[$index]}..."
  docker compose build "${services[$index]}"
done

# Copy the Docker images into K3s's separate containerd image store.
echo "Loading images into K3s..."
for repository in "${repositories[@]}"; do
  image_name="$DOCKER_ARCHIVE/$repository:$DOCKER_VERSION"
  echo "  Loading $image_name"
  docker image save "$image_name" |
    sudo k3s ctr --namespace k8s.io images import -
done

echo
echo "Local FASTDB images are ready."
echo "Use a Helm values file containing:"
echo "  imageRegistry: $DOCKER_ARCHIVE"
echo "  imageTag: $DOCKER_VERSION"
echo "  imagePullPolicy: Never"
echo "Then pass that values file to install-singlenode-fastdb.sh."
