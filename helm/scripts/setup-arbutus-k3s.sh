#!/usr/bin/env bash
# Set up a single-node K3s cluster on an Ubuntu VM (tested on 24.04).
set -euo pipefail

K3S_VERSION="v1.36.4+k3s1"
KUBECTL_VERSION="v1.36.4"
HELM_VERSION="v4.2.4"

if [[ $EUID -eq 0 ]]; then
  echo "Error: run this script as your normal user, not with sudo." >&2
  exit 1
fi

# shellcheck disable=SC1091
source /etc/os-release
if [[ ${ID:-} != "ubuntu" ]]; then
  echo "Error: this script supports Ubuntu only; found ${PRETTY_NAME:-unknown}." >&2
  exit 1
fi
if [[ ${VERSION_ID:-} != "24.04" ]]; then
  echo "Warning: this script was tested on Ubuntu 24.04; found ${PRETTY_NAME:-unknown}." >&2
fi

case "$(uname -m)" in
  x86_64) DOWNLOAD_ARCH="amd64" ;;
  aarch64|arm64) DOWNLOAD_ARCH="arm64" ;;
  *)
    echo "Error: unsupported CPU architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

SETUP_TMPDIR="$(mktemp -d)"
trap 'rm -rf "$SETUP_TMPDIR"' EXIT

echo "Installing required Ubuntu packages..."
sudo env DEBIAN_FRONTEND=noninteractive apt-get update
sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y \
  ca-certificates \
  curl \
  git \
  docker.io \
  docker-compose-v2

echo "Enabling Docker..."
sudo systemctl enable --now docker
if ! id -nG | tr ' ' '\n' | grep -Fxq docker; then
  sudo usermod -aG docker "$(id -un)"
  DOCKER_LOGIN_REQUIRED=true
else
  DOCKER_LOGIN_REQUIRED=false
fi

echo "Installing kubectl $KUBECTL_VERSION..."
KUBECTL_URL="https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$DOWNLOAD_ARCH/kubectl"
curl -fsSL "$KUBECTL_URL" -o "$SETUP_TMPDIR/kubectl"
curl -fsSL "$KUBECTL_URL.sha256" -o "$SETUP_TMPDIR/kubectl.sha256"
echo "$(<"$SETUP_TMPDIR/kubectl.sha256")  $SETUP_TMPDIR/kubectl" |
  sha256sum --check --status

# An earlier K3s installation may have created this link. Replace only that
# known link; never remove an unrelated kubectl installation here.
if [[ -L /usr/local/bin/kubectl ]] &&
   [[ "$(readlink -f /usr/local/bin/kubectl)" == "/usr/local/bin/k3s" ]]; then
  sudo unlink /usr/local/bin/kubectl
fi
sudo install -o root -g root -m 0755 "$SETUP_TMPDIR/kubectl" /usr/local/bin/kubectl

if command -v k3s >/dev/null 2>&1; then
  INSTALLED_K3S_VERSION="$(k3s --version | awk 'NR == 1 { print $3 }')"
  if [[ "$INSTALLED_K3S_VERSION" != "$K3S_VERSION" ]]; then
    echo "Error: K3s $INSTALLED_K3S_VERSION is installed; expected $K3S_VERSION." >&2
    echo "Refusing to replace an existing cluster automatically." >&2
    exit 1
  fi
  echo "K3s $K3S_VERSION is already installed."
else
  echo "Installing K3s $K3S_VERSION..."
  curl -fsSL https://get.k3s.io -o "$SETUP_TMPDIR/install-k3s.sh"
  sudo env \
    INSTALL_K3S_VERSION="$K3S_VERSION" \
    INSTALL_K3S_EXEC="server --disable traefik --disable servicelb" \
    INSTALL_K3S_SYMLINK="skip" \
    sh "$SETUP_TMPDIR/install-k3s.sh"
fi

echo "Configuring Kubernetes access for $(id -un)..."
mkdir -p "$HOME/.kube"
sudo install \
  -o "$(id -u)" \
  -g "$(id -g)" \
  -m 0600 \
  /etc/rancher/k3s/k3s.yaml \
  "$HOME/.kube/config"

echo "Waiting for the Kubernetes node to become ready..."
kubectl wait \
  --for=condition=Ready \
  "node/$(hostname)" \
  --timeout=120s

echo "Installing Helm $HELM_VERSION..."
HELM_ARCHIVE="helm-${HELM_VERSION}-linux-${DOWNLOAD_ARCH}.tar.gz"
HELM_URL="https://get.helm.sh/$HELM_ARCHIVE"
curl -fsSL "$HELM_URL" -o "$SETUP_TMPDIR/$HELM_ARCHIVE"
curl -fsSL "$HELM_URL.sha256sum" -o "$SETUP_TMPDIR/$HELM_ARCHIVE.sha256sum"
(
  cd "$SETUP_TMPDIR"
  sha256sum --check --status "$HELM_ARCHIVE.sha256sum"
  tar -xzf "$HELM_ARCHIVE"
)
sudo install -o root -g root -m 0755 \
  "$SETUP_TMPDIR/linux-${DOWNLOAD_ARCH}/helm" \
  /usr/local/bin/helm

echo
kubectl get nodes
kubectl get pods --all-namespaces
kubectl version --client
helm version --short
sudo docker info --format 'Docker server: {{.ServerVersion}}'
docker compose version
df -h /

echo
echo "K3s is ready."
if [[ "$DOCKER_LOGIN_REQUIRED" == true ]]; then
  echo "Log out and back in before running Docker without sudo."
fi
