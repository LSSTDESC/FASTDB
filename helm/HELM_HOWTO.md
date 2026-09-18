# FASTDB Helm Chart Guide

This guide explains how to deploy FASTDB to Kubernetes clusters using Helm.

## Conceptual Overview

### What is Helm?

Helm is a package manager for Kubernetes. It uses **charts** (packages of pre-configured Kubernetes resources) to deploy applications. Think of it like `apt` or `brew` but for Kubernetes.

### How FASTDB Helm Chart Works

```
helm/
├── scripts/                 # Local-development helper scripts
└── fastdb/
    ├── Chart.yaml           # Chart metadata (name, version)
    ├── values.yaml          # Default configuration values
    ├── values-local.yaml    # Local Kind cluster overrides
    ├── values-slac.yaml     # SLAC S3DF overrides
    └── templates/           # Kubernetes manifest templates
        ├── _helpers.tpl     # Reusable template functions
        ├── secrets.yaml
        ├── pvcs.yaml
        ├── postgres.yaml
        ├── mongodb.yaml
        ├── webap.yaml
        ├── single-broker-ingestion.yaml
        └── ...
```

**Key Concepts:**

1. **Templates** (`templates/*.yaml`) - Kubernetes manifests with placeholders like `{{ .Values.global.namespace }}`

2. **Values** (`values.yaml`) - Default configuration that fills in the template placeholders

3. **Value Overrides** (`values-*.yaml`) - Environment-specific settings that override defaults

4. **Rendering** - Helm combines templates + values to produce final Kubernetes manifests

```
┌─────────────┐     ┌─────────────┐     ┌──────────────────┐
│  Templates  │  +  │   Values    │  =  │ K8s Manifests    │
│  (*.yaml)   │     │ (values.yaml│     │ (ready to deploy)│
└─────────────┘     └─────────────┘     └──────────────────┘
```

## Quick Start

### Prerequisites

- Kubernetes cluster (Kind, SLAC S3DF, NERSC SPIN, etc.)
- `helm` CLI installed
- `kubectl` configured to access your cluster
- Docker Desktop for the recommended local Kind workflow. The general
  `helm-deploy.sh` script can also use Podman for other deployments.
- Docker images built and accessible
- For private registries (GHCR, NERSC, etc.): a GitHub PAT with `read:packages` scope (see [Registry Credentials](#registry-credentials))

The local helper scripts use Docker Compose and Docker image loading for Kind.
The general `scripts/helm-deploy.sh` supports Podman where the target
environment and Kind setup support it.

### Recommended Local Kind Workflow

For a laptop development environment, use the provided helpers from the
repository root:

```bash
./helm/scripts/create-local-cluster.sh
./helm/scripts/install-local-fastdb.sh
```

The first script only creates and checks the `fastdb-local` Kind cluster. The
second script builds FASTDB with `http://localhost:8080/` as its external URL,
builds and loads every required image, and installs the local Helm release.
Keeping these steps separate makes it possible to update FASTDB without
recreating the cluster or deleting its database volumes.

Re-run `install-local-fastdb.sh` after changing FASTDB Python code, database
migrations, container images, Helm templates, or local values. It recreates the
`createdb` migration Job on every run, but preserves the database itself.

After it finishes, check the deployment and open:

```bash
kubectl --context kind-fastdb-local get pods -n fastdb-local

# FASTDB web application
open http://localhost:8080

# MailHog inbox (password-reset emails)
open http://localhost:8025
```

To delete the local cluster and all of its local FASTDB data:

```bash
./helm/scripts/stop-local.sh
```

This is deliberately a reset, not a pause: the next
`create-local-cluster.sh` creates a fresh cluster and database environment.

To create the fixed local test account (`test_user`) and open its password-reset
page and MailHog message, run:

```bash
./helm/scripts/create-user-local.sh
```

Enter the desired password manually on the opened reset page.

The old combined workflow is temporarily retained as
`helm/scripts/deprecated-start-local.sh`. It is deprecated and should only be
used when reproducing the previous installation behavior.

### Arbutus development VM workflow

On an Ubuntu VM, set up the single-node K3s cluster once:

```bash
./helm/scripts/setup-arbutus-k3s.sh
```

To use the prebuilt images in CANFAR Harbor, log in once and install with the
Harbor values file:

```bash
docker login images.canfar.net
./helm/scripts/install-arbutus-fastdb.sh \
  ./helm/fastdb/values-arbutus-harbor.yaml
```

To build images from the current checkout instead, prepare the local Docker
and K3s images first, then install with the local values file:

```bash
./helm/scripts/build-local-images.sh \
  fastdb.local test20260428
./helm/scripts/install-arbutus-fastdb.sh \
  ./helm/fastdb/values-arbutus-dev.yaml
```

Create the initial FASTDB user without requiring browser access to MailHog:

```bash
./helm/scripts/create-user-arbutus.sh
```

The user script creates the server-side account only. It does not create a
`.fastdb.ini` file or a Kubernetes client Secret. Each application that uses
the FASTDB Python client is responsible for supplying its own credentials; a
Kubernetes application will typically create and mount a Secret containing its
`.fastdb.ini` file in that application's namespace.

The installer takes all Helm image settings from the selected values file. It
uses the configured shell image to prepare the host-mounted `install/` tree,
but it does not build deployment images. Kubernetes pulls Harbor images when
needed; locally built images must first be loaded with
`build-local-images.sh`.

The installer generates development passwords on its first run in the ignored,
mode-600 file `helm/fastdb/values-arbutus-secrets.yaml`; keep that file for
later upgrades. PostgreSQL standby is disabled. Alert ingestion expects a LASS
Kafka broker published on port 19092 of the same VM. The installer discovers
the K3s node's internal address and uses it to connect the FASTDB broker
consumer to LASS, so the values files do not contain a VM-specific IP address.

The K3s `local-path` provisioner stores the database PVCs on the VM's root
filesystem. The Arbutus flavour's ephemeral disk is deliberately unused. Check
space periodically with:

```bash
df -h /
sudo du -sh /var/lib/docker /var/lib/rancher/k3s
```

From inside the VM, FASTDB and MailHog are available at:

```text
http://localhost:30080
http://localhost:30025
```

**TODO: Configure access from outside the VM.** These addresses are currently
only reachable from within the VM. External access will require a floating IP
or another route to the VM.

`values-local.yaml` is specifically for the local/laptop setup.  In particular, it
contains host-network settings for a local Kafka broker (using the [LASS](https://github.com/CanDIAPL/lass) tool) and an ingestion configuration for the `lass-topic`; do not use it as the starting point for a
remote deployment.  Create a separate values file for each remote environment.

### Manual Local Kind Deployment

The deploy script can create a Kind cluster for you via `--create-cluster`. The Kind config template (`admin/local/kind-config.yaml`) uses `${PWD}` in `hostPath` entries, which the script replaces with the current directory at runtime. The cluster is named after the namespace, and `--context` is set to `kind-<namespace>` automatically.

```bash
# Create the Kind cluster, build the install artifact and images, load the
# images into Kind, and deploy. This can take several minutes on first run.
./scripts/helm-deploy.sh fastdb-local ./helm/fastdb/values-local.yaml \
  --create-cluster admin/local/kind-config.yaml \
  --external-url http://localhost:8080/

# Verify
kubectl --context kind-fastdb-local get pods -n fastdb-local
curl http://localhost:8080
```

On subsequent deploys, `--create-cluster` will skip creation if the cluster already exists. Or omit it and use `--context kind-fastdb-local` directly.

If you run the image-loading commands manually, create the Kind cluster
*before* invoking `kind load docker-image`; there is no node to receive images
until then.

To tear down the cluster:

```bash
kind delete cluster --name fastdb-local
```

### Registry Credentials

Deployments that pull from private registries (e.g., GHCR) need a `dockerconfigjson` secret. The Helm chart creates this automatically when `global.registryCredentials.enabled` is `true` in your values file. Pass the password at deploy time via `--registry-password` (or `--set`) so it never gets committed to git.

> **Note:** You can generate a GitHub Personal Access Token (PAT) with `read:packages` scope at https://github.com/settings/tokens.

Configure your values file with registry info (no password):

```yaml
global:
  imagePullSecrets:
    - name: ghcr-secret

  registryCredentials:
    enabled: true
    secretName: ghcr-secret
    server: ghcr.io
    username: your-github-username
    password: ""  # NEVER commit - pass via --registry-password or --set
```

Then pass the password at deploy time:

```bash
# Via the deploy script
./scripts/helm-deploy.sh my-namespace ./helm/fastdb/values-my-env.yaml \
  --registry-password <your-github-pat>

# Or via helm directly
helm upgrade --install fastdb ./helm/fastdb \
  -f ./helm/fastdb/values-my-env.yaml -n my-namespace --create-namespace \
  --set global.registryCredentials.password=<your-github-pat>
```

### Build the Install Directory

The `install/` directory is a build artifact produced by Automake. It contains the Python code, config files, and static assets that get mounted into pods at `/fastdb`. This directory is **not** in git — it must be built before deploying.

```bash
# Build install/ using Compose (runs ./configure && make install inside a container)
docker compose run --rm makeinstall
```

This creates/updates the `install/` directory at the repo root. The `db/` directory (SQL migrations) is already in git and doesn't need building.

#### External URL and Subdirectory Deployments

When FASTDB is served from a subdirectory (e.g., `https://host/fastdb-ccosta-dev/` instead of `https://host/`), the frontend JavaScript and HTML templates must be built with the correct base path. The Automake build system uses `@external_url@` placeholders in `.js.in` and `.html.in` files that get substituted during `./configure`:

```
# Template (fastdb.js.in):
import { rkWebUtil } from "@external_url@static/rkwebutil.js";

# Built with --with-external-url=/fastdb-ccosta-dev/ → (fastdb.js):
import { rkWebUtil } from "/fastdb-ccosta-dev/static/rkwebutil.js";

# Built without (default) → (fastdb.js):
import { rkWebUtil } from "static/rkwebutil.js";
```

The deploy script handles this via `--external-url`:

```bash
# Build with subdirectory path baked into frontend
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml \
  --external-url /fastdb-ccosta-dev/ --registry-password ghp_xxxxx
```

The `--external-url` value must end in a trailing slash.  It can be either a
path (such as `/fastdb-ccosta-dev/`) or a complete public URL (such as
`https://fastdb.example.org/fastdb-ccosta-dev/`).  Use the complete URL when
FASTDB needs to send an absolute link in email, including password-reset links.
For the local Kind helper, this is `http://localhost:8080/`.

Two settings work together:
- `--external-url /path/` or `https://host/path/` → bakes the URL/path into
  static JS/HTML at build time and into links generated by the server
- `webap.basePath: /path` → sets `SCRIPT_NAME` env var for Flask routing at runtime

For a root-path deployment, omit `webap.basePath`.  Pass the public root URL to
`--external-url` when generated emails must point to that public address.

### Deploy Script

`scripts/helm-deploy.sh` is a convenience script that wraps the manual steps (build, helm install, code copy, pod restart) into a single command. It is not required — everything it does can be run by hand (see [Common Operations](#common-operations)) — but it saves repetitive typing during development when you're deploying over and over.

```bash
./scripts/helm-deploy.sh [NAMESPACE] [VALUES_FILE] [OPTIONS]
```

**Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `NAMESPACE` | `local` | Kubernetes namespace |
| `VALUES_FILE` | `./helm/fastdb/values-local.yaml` | Helm values file |

**Options:**

| Option | Description |
|--------|-------------|
| `--create-cluster FILE` | Create a Kind cluster before deploying. `${PWD}` in the config is replaced with the current directory. Cluster name is set to `NAMESPACE`, context to `kind-NAMESPACE`. Skips creation if the cluster already exists. |
| `--context NAME` | Kubernetes context to use (default: current kubeconfig context). Set automatically by `--create-cluster`. |
| `--registry-password PAT` | Registry password/token (passed to Helm as `registryCredentials.password`) |
| `--external-url URL_OR_PATH` | Public URL or base path baked into generated frontend assets and links. It must end in `/`; its path component must match `webap.basePath` when one is configured. See [External URL and Subdirectory Deployments](#external-url-and-subdirectory-deployments). |
| `--skip-build` | Skip the `docker-compose makeinstall` step |
| `--skip-helm` | Skip `helm upgrade --install` (just copy code + restart) |
| `--release NAME` | Helm release name (default: `fastdb`) |
| `-h, --help` | Show help |

**Examples:**

```bash
# Full deploy from scratch (build + install + copy + restart)
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml \
  --external-url /fastdb-ccosta-dev/ --registry-password ghp_xxxxx

# Skip build (install/ already up to date)
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml \
  --skip-build --registry-password ghp_xxxxx

# Code-only update (no build, no helm, just copy code and restart pods)
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml \
  --skip-build --skip-helm

# Config-only update (no build, re-run helm upgrade, copy code, restart)
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml \
  --skip-build --registry-password ghp_xxxxx

# Root-path deploy (no subdirectory, no --external-url needed)
./scripts/helm-deploy.sh my-namespace ./helm/fastdb/values-my-env.yaml \
  --registry-password ghp_xxxxx
```

**What the script does:**

1. Builds `install/` via `docker-compose run makeinstall`
2. Runs `helm upgrade --install` with `--create-namespace` (works for fresh installs and upgrades; creates the namespace if it doesn't exist)
3. Waits for the shell pod to be ready
4. Copies `install/` contents to `/fastdb/` on the code PVC via tar pipe through the shell pod
5. Copies `db/` contents to `/fastdb/db/` on the code PVC
6. Restarts webap and queryrunner deployments so they pick up the new code
7. Prints pod status

The Helm release is stored in the target namespace (not `default`). The createdb Job retries automatically (`restartPolicy: OnFailure`) until postgres and code are both ready.

> **Note:** `--registry-password` is only needed when running the helm step. If you use `--skip-helm`, the secret already exists in the cluster and no password is required.

### Deploy to SLAC S3DF

```bash
# Ensure kubectl context points to SLAC cluster
kubectl config use-context your-slac-context

# Deploy with subdirectory path (if using basePath in values)
./scripts/helm-deploy.sh your-namespace ./helm/fastdb/values-slac.yaml \
  --external-url /your-base-path/ --registry-password <your-github-pat>
```

The script handles namespace creation, registry credentials, frontend path configuration, code copying, and pod restarts.

## Common Operations

### Install a New Release

You can use the [deploy script](#deploy-script) to run all the steps in one command, or run helm directly:

```bash
helm upgrade --install <release-name> ./helm/fastdb \
  -f ./helm/fastdb/values-<env>.yaml -n <namespace> --create-namespace
```

> **Important:** Always pass `-n <namespace>` so the Helm release is stored in the correct namespace (not `default`). Use `--create-namespace` if the namespace may not exist yet.

After `helm install`, the code PVC will be empty. You still need to copy `install/` and `db/` to the PVC via the shell pod (the deploy script does this automatically, or see [What the script does](#deploy-script) for the manual commands).

### Upgrade an Existing Release

After modifying values or templates:

```bash
helm upgrade <release-name> ./helm/fastdb \
  -f ./helm/fastdb/values-<env>.yaml -n <namespace>
```

Or with the deploy script: `--skip-build` to skip the build step when only values/templates changed.

### View Current Values

```bash
helm get values <release-name> -n <namespace>
```

### Preview Changes (Dry Run)

See what would be deployed without actually deploying:

```bash
helm template <release-name> ./helm/fastdb -f ./helm/fastdb/values-<env>.yaml
```

Or with diff against current deployment:

```bash
helm upgrade <release-name> ./helm/fastdb \
  -f ./helm/fastdb/values-<env>.yaml -n <namespace> --dry-run
```

### Uninstall a Release

```bash
helm uninstall <release-name> -n <namespace>
```

> **Warning:** This deletes the namespace and everything in it (including the registry secret and PVCs). A fresh `helm-deploy.sh` with `--registry-password` will recreate everything, but data in PVCs will be lost.

### List Releases

```bash
helm list -n <namespace>
helm list -A  # All namespaces
```

### Rollback to Previous Version

```bash
helm rollback <release-name> <revision-number> -n <namespace>
helm history <release-name> -n <namespace>  # View revision history
```

## Configuration Reference

### Global Settings

```yaml
global:
  namespace: fastdb-local          # Kubernetes namespace
  environment: dev                 # Environment label

  imageRegistry: "localhost"       # Image registry prefix
  imageTag: "local"                # Default image tag
  imagePullPolicy: Never           # Never, Always, IfNotPresent
  imagePullSecrets: []             # Registry credentials
  # - name: ghcr-secret

  # Helm-managed registry credentials (creates imagePullSecret automatically)
  registryCredentials:
    enabled: false                 # Set true to create the secret via Helm
    secretName: ghcr-secret        # Must match imagePullSecrets[].name
    server: ""                     # e.g., ghcr.io
    username: ""                   # Registry username
    password: ""                   # NEVER commit - pass via --registry-password

  namespaceLabels: {}              # Additional namespace labels
  # owner: username
```

### Volume Configuration

```yaml
volumes:
  type: pvc                        # Always "pvc" for database storage
  storageClass: ""                 # Storage class (empty = default)
  codeHostPath: false              # Mount code from host (Kind only)
```

### Component Configuration

Each component (postgres, mongodb, webap, etc.) follows this pattern:

```yaml
postgres:
  enabled: true                    # Enable/disable component
  image:
    repository: fastdb-postgres    # Image name
    tag: ""                        # Tag override (uses global.imageTag if empty)
  replicas: 1

  persistence:
    size: 10Gi
    accessMode: ReadWriteOnce

  externalAccess:
    enabled: false
    type: NodePort                 # NodePort or LoadBalancer
    nodePort: 30432                # For NodePort type
    annotations: {}                # For LoadBalancer (e.g., metallb)
```

### Secrets

```yaml
secrets:
  postgres:
    password: "changeme"
    roPassword: "changeme"
    replicatorPassword: "changeme"
  secretKey: "changeme"

  mongodb:
    adminUser: "admin"
    adminPassword: "changeme"
    # ... etc
```

**Important:** Never commit real passwords to git. Use `--set` flags or external secret management:

```bash
helm install fastdb ./helm/fastdb \
  -f ./helm/fastdb/values-slac.yaml \
  --set secrets.postgres.password="real-password" \
  --set secrets.secretKey="real-secret"
```

## Creating a New Environment

### 1. Create Values File

Copy an existing values file as a starting point:

```bash
cp helm/fastdb/values-local.yaml helm/fastdb/values-myenv.yaml
```

### 2. Customize Settings

Edit the new file. Key sections to modify:

```yaml
# values-myenv.yaml

global:
  namespace: fastdb-myenv          # Unique namespace
  environment: myenv

  # Image source
  imageRegistry: "ghcr.io/myorg"   # Your registry
  imageTag: "v1.0.0"               # Your tag
  imagePullPolicy: Always
  imagePullSecrets:
    - name: my-registry-secret    # If private registry

# Enable/disable components as needed
postgres:
  enabled: true
  persistence:
    size: 50Gi                     # Adjust storage

mongodb:
  enabled: true                    # Or false if not needed

webap:
  enabled: true
  externalAccess:
    enabled: true
    type: LoadBalancer             # Or NodePort

# Disable dev-only components for production
mailhog:
  enabled: false

shell:
  enabled: false
```

### 3. Deploy

```bash
./scripts/helm-deploy.sh myenv ./helm/fastdb/values-myenv.yaml \
  --registry-password <your-pat>
```

Or if using raw helm (you'll need to copy code to the PVC separately):

```bash
helm upgrade --install fastdb ./helm/fastdb \
  -f ./helm/fastdb/values-myenv.yaml -n myenv --create-namespace
```

## Environment Examples

### Local Development (Kind)

```yaml
# values-local.yaml highlights
global:
  imageRegistry: "localhost"
  imageTag: "local"
  imagePullPolicy: Never           # Images loaded into Kind

volumes:
  codeHostPath: true               # Mount code from host

postgres:
  externalAccess:
    enabled: true
    type: NodePort
    nodePort: 30432
```

Kind config (`admin/local/kind-config.yaml`) must include:

```yaml
extraMounts:
  - hostPath: /path/to/FASTDB/install
    containerPath: /fastdb-install
  - hostPath: /path/to/FASTDB/db
    containerPath: /fastdb-db
extraPortMappings:
  - containerPort: 30080
    hostPort: 8080
```

### SLAC S3DF

```yaml
# values-slac.yaml highlights
global:
  imageRegistry: "ghcr.io/fifteen3"
  imageTag: "latest"
  imagePullSecrets:
    - name: ghcr-secret

postgres:
  externalAccess:
    enabled: true
    type: LoadBalancer
    annotations:
      metallb.io/address-pool: sdf-services

  walArchive:
    enabled: true                  # For replication
    size: 20Gi

walWebserver:
  enabled: true                    # Serve WAL files
  ingress:
    enabled: true
    host: desc-fastdb.slac.stanford.edu
```

### NERSC SPIN (Production)

```yaml
# values-spin-prod.yaml (example)
global:
  imageRegistry: "registry.nersc.gov/m1727/raknop"
  imageTag: "dp1"
  imagePullSecrets:
    - name: registry-nersc

volumes:
  type: pvc
  storageClass: nfs-client

postgres:
  persistence:
    size: 2048Gi                   # Large production storage
  sharedMemory: 128Gi
```

## Troubleshooting

### Pods Not Starting

```bash
# Check pod status
kubectl get pods -n <namespace>

# Describe pod for events
kubectl describe pod <pod-name> -n <namespace>

# Check logs
kubectl logs <pod-name> -n <namespace>
```

### PVC Issues

```bash
# Check PVC status
kubectl get pvc -n <namespace>

# Describe for events
kubectl describe pvc <pvc-name> -n <namespace>
```

### Image Pull Errors

```bash
# Verify image exists
docker images | grep fastdb

# For Kind, ensure images are loaded
kind load docker-image <image> --name <cluster>

# For private registries, check secret
kubectl get secret <pull-secret> -n <namespace>
```

### Template Errors

```bash
# Lint chart
helm lint ./helm/fastdb -f ./helm/fastdb/values-<env>.yaml

# Render and inspect
helm template fastdb ./helm/fastdb -f ./helm/fastdb/values-<env>.yaml > /tmp/rendered.yaml
```

### Database Connection Issues

```bash
# Check postgres is running
kubectl get pods -n <namespace> -l app=postgres

# Test connection from shell pod
kubectl exec -it deploy/shell -n <namespace> -- \
  psql -h postgres -U postgres -c "SELECT 1"
```

## Best Practices

1. **Never commit secrets** - Use `--set` flags or external secret management

2. **Use specific image tags** - Avoid `latest` in production

3. **Test with dry-run** - Preview changes before applying

4. **Version your values files** - Keep environment configs in git

5. **Use namespaces** - Isolate environments with separate namespaces

6. **Monitor resources** - Set appropriate resource requests/limits

7. **Backup PVCs** - Database PVCs contain critical data

## File Reference

| File | Purpose |
|------|---------|
| `Chart.yaml` | Chart metadata |
| `values.yaml` | Default values (don't modify for deployment) |
| `values-local.yaml` | Local Kind deployment |
| `values-ccosta-dev.yaml` | SLAC ccosta-dev deployment |
| `values-slac.yaml` | SLAC S3DF deployment |
| `templates/_helpers.tpl` | Reusable template functions |
| `templates/namespace.yaml` | Namespace resource |
| `templates/secrets.yaml` | Secrets and ConfigMaps |
| `templates/pvcs.yaml` | Persistent Volume Claims |
| `templates/postgres.yaml` | PostgreSQL deployment + services |
| `templates/postgres-standby.yaml` | PostgreSQL read replica |
| `templates/mongodb.yaml` | MongoDB deployment + service |
| `templates/webap.yaml` | Web application |
| `templates/shell.yaml` | Debug shell pod |
| `templates/queryrunner.yaml` | Query runner service |
| `templates/mailhog.yaml` | Email testing (dev) |
| `templates/createdb-job.yaml` | Database migration job |
| `templates/wal-webserver.yaml` | WAL archive webserver (SLAC) |
