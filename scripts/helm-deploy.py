#!/usr/bin/env python3
"""
Deploy FASTDB to Kubernetes via Helm.

This is a Python replacement for scripts/helm-deploy.sh with a cleaner split between:

- Kubernetes namespace: where Helm installs resources
- Kind cluster name: only used for local Kind clusters
- Kubernetes context: kubectl/helm context
- Docker image tag: used for build/load; Helm tags should normally come from values files

Usage:
  ./scripts/helm-deploy.py [NAMESPACE] [VALUES_FILE] [OPTIONS]

Examples:
  # Local Kind bootstrap using the built-in Kind config
  DOCKER_VERSION=test20260225 ./scripts/helm-deploy.py fastdb-local admin/helm/fastdb/values-local.yaml \
    --create-cluster \
    --load-images

  # Local Kind bootstrap using a custom Kind config
  DOCKER_VERSION=test20260225 ./scripts/helm-deploy.py fastdb-local admin/helm/fastdb/values-local.yaml \
    --cluster-name fastdb-local \
    --create-cluster admin/local/kind-config.yaml \
    --load-images

  # Existing Kubernetes context, e.g. NERSC/SLAC
  DOCKER_VERSION=test20260225 ./scripts/helm-deploy.py rearmstr-dev admin/helm/fastdb/values-nersc.yaml \
    --context <your-context>

Notes:
  Namespace and image tag should normally be set in the Helm values file.
  This script does not force global.namespace or global.imageTag unless you explicitly pass --set-value.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable


DEFAULT_VALUES = "admin/helm/fastdb/values-local.yaml"
DEFAULT_CHART = "admin/helm/fastdb"
DEFAULT_RELEASE = "fastdb"
DEFAULT_DOCKER_ARCHIVE = "ghcr.io/lsstdesc"
DEFAULT_DOCKER_VERSION = "test20260225"
DEFAULT_KIND_VOLUME_ROOT = "kind-volumes"
DEFAULT_KIND_CONFIG_SENTINEL = "__DEFAULT_KIND_CONFIG__"

# FASTDB local Kind assumptions. These host directories are mounted into the
# Kind node. The Helm chart can then use hostPath volumes for /fastdb-install
# and /fastdb-db without relying on docker exec mkdir after cluster creation.
DEFAULT_KIND_CONFIG_TEMPLATE = r"""
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4

nodes:
  - role: control-plane

    extraMounts:
      - hostPath: ${PWD}/kind-volumes/fastdb-install
        containerPath: /fastdb-install

      - hostPath: ${PWD}/kind-volumes/fastdb-db
        containerPath: /fastdb-db

    extraPortMappings:
      # ingress/http
      - containerPort: 30080
        hostPort: 8080
        protocol: TCP

      # ingress/https
      - containerPort: 30443
        hostPort: 8443
        protocol: TCP

      # postgres external access
      - containerPort: 30432
        hostPort: 5432
        protocol: TCP

      # minio site1 api
      - containerPort: 30090
        hostPort: 9000
        protocol: TCP

      # minio site1 console
      - containerPort: 30091
        hostPort: 9001
        protocol: TCP

      # minio site2 api
      - containerPort: 30092
        hostPort: 9100
        protocol: TCP

      # minio site2 console
      - containerPort: 30093
        hostPort: 9101
        protocol: TCP
""".lstrip()

# Compose build service names. shell is intentionally omitted: current compose
# config has shell as image-only, not buildable.
BUILD_IMAGES = [
    "postgres",
    "postgres-standby",
    "mongodb",
    "webap",
    "queryrunner",
]

# Image suffixes as tagged in GHCR/local Docker. queryrunner's image is
# fastdb-query-runner, while its compose service is queryrunner.
LOAD_IMAGES = [
    "postgres",
    "postgres-standby",
    "mongodb",
    "shell",
    "webap",
    "query-runner",
]


class DeployError(RuntimeError):
    pass


def run(
    cmd: list[str],
    *,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
    check: bool = True,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    printable = " ".join(cmd)
    if input_text is None:
        print(f"+ {printable}")
    else:
        print(f"+ {printable}  # stdin supplied")

    return subprocess.run(
        cmd,
        input=input_text,
        text=True,
        env=env,
        check=check,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )


def require_cmd(name: str) -> None:
    if shutil.which(name) is None:
        raise DeployError(f"Required command not found in PATH: {name}")


def detect_container_runtime() -> str:
    if shutil.which("podman"):
        return "podman"
    if shutil.which("docker"):
        return "docker"
    raise DeployError("podman or docker is required, but neither was found in PATH")


def kubectl_base(context: str | None) -> list[str]:
    cmd = ["kubectl"]
    if context:
        cmd += ["--context", context]
    return cmd


def helm_context_args(context: str | None) -> list[str]:
    if context:
        return ["--kube-context", context]
    return []


def kind_cluster_exists(cluster_name: str) -> bool:
    proc = run(["kind", "get", "clusters"], check=False, capture=True)
    if proc.returncode != 0:
        return False
    return cluster_name in {line.strip() for line in proc.stdout.splitlines()}


def prepare_default_kind_host_dirs(volume_root: Path) -> None:
    print("--- Preparing default Kind hostPath directories ---")
    for subdir in ["fastdb-install", "fastdb-db"]:
        path = volume_root / subdir
        if path.exists() and not path.is_dir():
            raise DeployError(f"Expected directory but found non-directory path: {path}")
        path.mkdir(parents=True, exist_ok=True)
        print(f"  {path}")


def get_kind_config_text(create_cluster_arg: str, volume_root: Path) -> str:
    if create_cluster_arg == DEFAULT_KIND_CONFIG_SENTINEL:
        prepare_default_kind_host_dirs(volume_root)
        return DEFAULT_KIND_CONFIG_TEMPLATE

    config_path = Path(create_cluster_arg)
    if not config_path.is_file():
        raise DeployError(f"Kind config not found: {config_path}")
    return config_path.read_text()


def create_kind_cluster(cluster_name: str, create_cluster_arg: str, volume_root: Path) -> str:
    require_cmd("kind")

    if kind_cluster_exists(cluster_name):
        print(f"Kind cluster '{cluster_name}' already exists, skipping creation.")
    else:
        config_text = get_kind_config_text(create_cluster_arg, volume_root)
        config_text = config_text.replace("${PWD}", str(Path.cwd()))

        if create_cluster_arg == DEFAULT_KIND_CONFIG_SENTINEL:
            print(f"--- Creating Kind cluster '{cluster_name}' from built-in default config ---")
        else:
            print(f"--- Creating Kind cluster '{cluster_name}' from {create_cluster_arg} ---")

        run(["kind", "create", "cluster", "--name", cluster_name, "--config", "-"], input_text=config_text)

    return f"kind-{cluster_name}"


def build_install(container_runtime: str, external_url: str) -> None:
    if external_url:
        print(f"--- Building install/ with --with-external-url={external_url} ---")
        script = f"""
set -e

touch aclocal.m4 configure
find . -name Makefile.am -exec touch {{}} \;
find . -name Makefile.in -exec touch {{}} \;
./configure \
  --with-installdir=/fastdb \
  --with-smtp-server=mailhog \
  --with-smtp-port=1025 \
  --with-external-url={external_url}
make install
""".strip()
        run([container_runtime, "compose", "run", "--rm", "--entrypoint", "", "makeinstall", "/bin/bash", "-c", script])
    else:
        print("--- Building install/ via docker compose makeinstall ---")
        run([container_runtime, "compose", "run", "--rm", "makeinstall"])


def verify_install_tree() -> None:
    install = Path("install")
    if not install.is_dir() or not any(install.iterdir()):
        raise DeployError(
            "install/ directory is missing or empty. "
            "Run 'docker compose run --rm makeinstall' first, or do not use --skip-build."
        )


def verify_db_tree() -> None:
    db = Path("db")
    if not db.is_dir():
        raise DeployError("db/ directory is missing; cannot copy database support files into /fastdb/db")


def build_images(container_runtime: str, env: dict[str, str]) -> None:
    print("--- Building container images ---")
    run([container_runtime, "compose", "build", *BUILD_IMAGES], env=env)


def image_exists(container_runtime: str, image: str) -> bool:
    proc = run([container_runtime, "image", "inspect", image], check=False, capture=True)
    return proc.returncode == 0


def pull_image(container_runtime: str, image: str, env: dict[str, str]) -> None:
    print(f"  Local image missing; trying pull: {image}")
    run([container_runtime, "pull", image], env=env)


def load_images_into_kind(
    container_runtime: str,
    cluster_name: str,
    docker_archive: str,
    docker_version: str,
    env: dict[str, str],
) -> None:
    require_cmd("kind")
    print(f"--- Loading images into Kind cluster '{cluster_name}' ---")

    for img in LOAD_IMAGES:
        local_tag = f"{docker_archive}/fastdb-{img}:{docker_version}"
        print(f"  Loading {local_tag}")

        if not image_exists(container_runtime, local_tag):
            pull_image(container_runtime, local_tag, env)

        with tempfile.NamedTemporaryFile(prefix=f"fastdb-kind-{img}-", suffix=".tar", delete=False) as tmp:
            tar_path = tmp.name

        try:
            run([container_runtime, "save", "-o", tar_path, local_tag], env=env)
            run(["kind", "load", "image-archive", tar_path, "--name", cluster_name])
        finally:
            Path(tar_path).unlink(missing_ok=True)


def setup_kind_certs(namespace: str) -> None:
    script = Path("scripts/setup-kind-certs.sh")
    if not script.exists():
        print("--- setup-kind-certs.sh not found; skipping cert setup ---")
        return

    print("--- Setting up MinIO certs ---")
    run([str(script), namespace])


def helm_upgrade(
    release: str,
    chart: Path,
    values: Path,
    namespace: str,
    context: str | None,
    registry_password: str,
    extra_set_values: list[str],
) -> None:
    print("--- Running helm upgrade --install ---")

    set_args: list[str] = []
    if registry_password:
        set_args += ["--set", f"global.registryCredentials.password={registry_password}"]
    for item in extra_set_values:
        set_args += ["--set", item]

    run(
        [
            "helm",
            "upgrade",
            "--install",
            release,
            str(chart),
            "-f",
            str(values),
            "--create-namespace",
            "-n",
            namespace,
            *helm_context_args(context),
            *set_args,
        ]
    )


def wait_for_shell(namespace: str, context: str | None) -> str:
    print("--- Waiting for shell pod to be ready ---")
    k = kubectl_base(context)

    run(k + ["wait", "--for=condition=available", "deployment/shell", "-n", namespace, "--timeout=120s"])

    proc = run(
        k + ["get", "pods", "-n", namespace, "-l", "app=shell", "-o", "jsonpath={.items[0].metadata.name}"],
        capture=True,
    )
    pod = proc.stdout.strip()
    if not pod:
        raise DeployError("Could not find shell pod after deployment became available")

    print(f"  Shell pod: {pod}")
    return pod


def tar_copy_to_pod(source_dir: str, namespace: str, pod: str, dest_dir: str, context: str | None) -> None:
    k = kubectl_base(context)
    env = os.environ.copy()
    env["COPYFILE_DISABLE"] = "1"

    tar_proc = subprocess.Popen(
        ["tar", "cf", "-", "-C", source_dir, "."],
        stdout=subprocess.PIPE,
        env=env,
    )
    assert tar_proc.stdout is not None

    kubectl_proc = subprocess.run(
        k + ["exec", "-i", "-n", namespace, pod, "--", "tar", "xf", "-", "-C", dest_dir],
        stdin=tar_proc.stdout,
        text=False,
    )
    tar_proc.stdout.close()
    tar_return = tar_proc.wait()

    if tar_return != 0:
        raise DeployError(f"tar failed while copying {source_dir}")
    if kubectl_proc.returncode != 0:
        raise DeployError(f"kubectl exec tar failed while copying {source_dir} to {dest_dir}")


def copy_runtime_tree(namespace: str, shell_pod: str, context: str | None) -> None:
    k = kubectl_base(context)

    print("--- Copying install/ contents to /fastdb/ on PVC ---")
    tar_copy_to_pod("install", namespace, shell_pod, "/fastdb/", context)
    print("  install/ copied.")

    print("--- Copying db/ contents to /fastdb/db/ on PVC ---")
    run(k + ["exec", "-n", namespace, shell_pod, "--", "mkdir", "-p", "/fastdb/db"])
    tar_copy_to_pod("db", namespace, shell_pod, "/fastdb/db/", context)
    print("  db/ copied.")
    

def sync_db_to_kind_node(container_runtime: str, cluster_name: str) -> None:
    node = f"{cluster_name}-control-plane"

    print("--- Syncing db/ into Kind node /fastdb-db before Helm ---")
    run([container_runtime, "exec", node, "rm", "-rf", "/fastdb-db"])
    run([container_runtime, "exec", node, "mkdir", "-p", "/fastdb-db"])

    tar_proc = subprocess.Popen(
        ["tar", "cf", "-", "-C", "db", "."],
        stdout=subprocess.PIPE,
    )
    assert tar_proc.stdout is not None

    copy_proc = subprocess.run(
        [container_runtime, "exec", "-i", node, "tar", "xf", "-", "-C", "/fastdb-db"],
        stdin=tar_proc.stdout,
    )
    tar_proc.stdout.close()
    tar_return = tar_proc.wait()

    if tar_return != 0 or copy_proc.returncode != 0:
        raise DeployError("Failed to sync db/ into Kind node /fastdb-db")
    
def sync_db_to_kind_hostpath(volume_root: Path) -> None:
    src = Path("db")
    dest = volume_root / "fastdb-db"

    print(f"--- Syncing db/ into hostPath {dest} before Helm ---")

    if not src.is_dir():
        raise DeployError("db/ directory is missing; cannot sync to Kind hostPath")

    if dest.exists() and not dest.is_dir():
        raise DeployError(f"Expected directory but found non-directory path: {dest}")

    dest.mkdir(parents=True, exist_ok=True)

    for child in dest.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()

    shutil.copytree(src, dest, dirs_exist_ok=True)


def restart_optional_deployment(name: str, namespace: str, context: str | None) -> None:
    k = kubectl_base(context)
    restart = run(k + ["rollout", "restart", f"deployment/{name}", "-n", namespace], check=False)
    if restart.returncode != 0:
        print(f"  ({name} deployment not found or not enabled, skipping)")
        return

    status = run(k + ["rollout", "status", f"deployment/{name}", "-n", namespace, "--timeout=120s"], check=False)
    if status.returncode != 0:
        print(f"  ({name} rollout did not complete cleanly; inspect with kubectl describe/logs)")


def show_status(namespace: str, context: str | None) -> None:
    print("--- Pod status ---")
    run(kubectl_base(context) + ["get", "pods", "-n", namespace])


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deploy FASTDB to Kubernetes via Helm")
    parser.add_argument("namespace", nargs="?", default="fastdb-local", help="Kubernetes namespace to install into")
    parser.add_argument("values_file", nargs="?", default=DEFAULT_VALUES, help="Helm values file")

    parser.add_argument("--chart", default=DEFAULT_CHART, help="Helm chart path")
    parser.add_argument("--release", default=DEFAULT_RELEASE, help="Helm release name")
    parser.add_argument("--cluster-name", default=None, help="Kind cluster name. Defaults to namespace for local Kind.")
    parser.add_argument("--context", default=None, help="Kubernetes context to use")
    parser.add_argument(
        "--create-cluster",
        nargs="?",
        const=DEFAULT_KIND_CONFIG_SENTINEL,
        default=None,
        metavar="KIND_CONFIG",
        help="Create Kind cluster. With no file, use the built-in FASTDB local Kind config.",
    )
    parser.add_argument(
        "--kind-volume-root",
        default=DEFAULT_KIND_VOLUME_ROOT,
        help="Host directory root for built-in Kind config extraMounts",
    )
    parser.add_argument("--load-images", action="store_true", help="Build/load local images into Kind")
    parser.add_argument("--skip-build", action="store_true", help="Skip makeinstall build step")
    parser.add_argument("--skip-helm", action="store_true", help="Skip helm upgrade/install")
    parser.add_argument("--skip-certs", action="store_true", help="Skip scripts/setup-kind-certs.sh")
    parser.add_argument("--registry-password", default="", help="Registry password/token for Helm imagePullSecret")
    parser.add_argument("--external-url", default="/", help="Base path baked into frontend build")
    parser.add_argument("--docker-archive", default=os.environ.get("DOCKER_ARCHIVE", DEFAULT_DOCKER_ARCHIVE))
    parser.add_argument("--docker-version", default=os.environ.get("DOCKER_VERSION", DEFAULT_DOCKER_VERSION))
    parser.add_argument(
        "--set-value",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra Helm --set override. Can be repeated. Normally namespace/tag should live in values files.",
    )
    parser.add_argument(
        "--sync-kind-hostpaths",
        action="store_true",
        help="Sync db/ into the Kind node /fastdb-db hostPath before Helm. Use for existing Kind clusters.",
    )

    return parser.parse_args(list(argv))


def main(argv: Iterable[str]) -> int:
    args = parse_args(argv)

    namespace = args.namespace
    values = Path(args.values_file)
    chart = Path(args.chart)
    cluster_name = args.cluster_name or namespace
    kube_context = args.context
    load_images = args.load_images
    kind_volume_root = Path(args.kind_volume_root)

    require_cmd("kubectl")
    require_cmd("helm")
    container_runtime = detect_container_runtime()

    if not values.is_file():
        raise DeployError(f"values file not found: {values}")
    if not chart.is_dir():
        raise DeployError(f"chart directory not found: {chart}")

    env = os.environ.copy()
    env["DOCKER_ARCHIVE"] = args.docker_archive
    env["DOCKER_VERSION"] = args.docker_version

    if args.create_cluster:
        kube_context = create_kind_cluster(cluster_name, args.create_cluster, kind_volume_root)
        load_images = True

    current_context = kube_context
    if not current_context:
        proc = run(["kubectl", "config", "current-context"], check=False, capture=True)
        current_context = proc.stdout.strip() if proc.returncode == 0 else "(unknown)"

    print("=== FASTDB Helm Deploy ===")
    print(f"  Kubernetes namespace : {namespace}")
    print(f"  Kind cluster name    : {cluster_name}")
    print(f"  Values               : {values}")
    print(f"  Chart                : {chart}")
    print(f"  Release              : {args.release}")
    print(f"  Runtime              : {container_runtime}")
    print(f"  Context              : {current_context}")
    print(f"  Docker archive       : {args.docker_archive}")
    print(f"  Docker version       : {args.docker_version}")
    print(f"  External URL         : {args.external_url}")
    if args.create_cluster == DEFAULT_KIND_CONFIG_SENTINEL:
        print(f"  Kind volume root     : {kind_volume_root}")
    print("")

    if not args.skip_build:
        build_install(container_runtime, args.external_url)
        print("")


    verify_install_tree()
    verify_db_tree()

    if load_images:
        build_images(container_runtime, env)
        print("")
        load_images_into_kind(container_runtime, cluster_name, args.docker_archive, args.docker_version, env)
        print("")

    if not args.skip_certs and args.create_cluster:
        setup_kind_certs(namespace)
        print("")
        
    sync_kind_hostpaths = args.sync_kind_hostpaths or bool(args.create_cluster)
    if sync_kind_hostpaths:
        sync_db_to_kind_hostpath(kind_volume_root)

    if not args.skip_helm:
        helm_upgrade(
            args.release,
            chart,
            values,
            namespace,
            kube_context,
            args.registry_password,
            args.set_value,
        )
        print("")

    shell_pod = wait_for_shell(namespace, kube_context)
    print("")

    copy_runtime_tree(namespace, shell_pod, kube_context)
    print("")

    print("--- Restarting webap and queryrunner ---")
    restart_optional_deployment("webap", namespace, kube_context)
    restart_optional_deployment("queryrunner", namespace, kube_context)
    print("")

    show_status(namespace, kube_context)
    print("\n=== Deploy complete ===")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except subprocess.CalledProcessError as exc:
        print(f"ERROR: command failed with exit code {exc.returncode}: {' '.join(exc.cmd)}", file=sys.stderr)
        if exc.stdout:
            print(exc.stdout, file=sys.stderr)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        raise SystemExit(exc.returncode)
    except DeployError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
