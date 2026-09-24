# FASTDB Helm Chart Guide

This guide explains how to deploy FASTDB to Kubernetes clusters using Helm.

## Quickstart

### Option 1: Install scripts

Some site-specific installation scripts are available in [`helm/scripts/`](./scripts/). For example you can install fastdb on a single node kubernetes cluster with:

```bash
./helm/scripts/install-singlenode-fastdb.sh <values-file>
```

where the values file describes the specifics of the installation e.g. broker info, storage allocated to the data base, version numbers of images. Example values files are in [`helm/fastdb/`](./fastdb/), including [values-arbutus-dev.yaml](./fastdb/values-arbutus-dev.yaml) which contains the config for a small developement install simulated alerts from [LASS](https://github.com/CanDIAPL/lass).

### Option 2: Direct install with helm scripts

A FASTDB Helm deployment has two main stages: generate the runnable FASTDB
files with the `makeinstall` container, then deploy the Kubernetes resources
with Helm.

#### 1. Generate `install/` with the `makeinstall` container

The `makeinstall` Docker Compose service uses the FASTDB shell image.

Obtain the shell image by either pulling a prebuilt image:

```bash
docker pull "<path-to-shell-image-on-registry>"
```

or building it from the current checkout:

```bash
docker compose build shell
```

Then run the container that configures FASTDB and writes the runnable files to
`install/`:

```bash
USERID="$(id -u)" GROUPID="$(id -g)" \
  docker compose run --rm makeinstall
```

#### 2. Install FASTDB with Helm

Choose an environment-specific values file and run Helm:

```bash
helm upgrade --install fastdb ./helm/fastdb \
  --namespace <namespace> \
  --create-namespace \
  --values ./helm/fastdb/<values-file>.yaml
```

Example values files are in [`helm/fastdb/`](./fastdb/), including local Kind
and single-node Arbutus configurations. Some environments also require a secrets
values file, registry credentials, storage settings, or host-path overrides;
the environment-specific sections below describe those additions.

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
    ├── values-arbutus.yaml  # Single-node VM overrides
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

## Details

### Prerequisites

- Single-node Kubernetes cluster, or a local Kind cluster
- `helm` CLI installed
- `kubectl` configured to access your cluster
- Docker images built and accessible
- Credentials for any private image registry (see [Registry Credentials](#registry-credentials))

The local helper scripts use Docker Compose and Docker image loading for Kind.

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

### Arbutus development VM workflow

On an Ubuntu VM, set up the single-node K3s cluster once:

```bash
./helm/scripts/setup-arbutus-k3s.sh
```

To use prebuilt images from a private registry, log in once and install with a
values file that identifies the registry, tag, and `fastdb-registry` pull
secret. The included example uses the CanDIAPL project in CANFAR Harbor:

```bash
docker login images.canfar.net
./helm/scripts/install-singlenode-fastdb.sh \
  ./helm/fastdb/values-arbutus.yaml
```

To build images from the current checkout instead, prepare the local Docker
and K3s images first, then install with the local values file:

```bash
./helm/scripts/build-local-images.sh \
  fastdb.local test20260428
./helm/scripts/install-singlenode-fastdb.sh \
  ./helm/fastdb/values-arbutus-dev.yaml
```

Create the initial FASTDB user without requiring browser access to MailHog:

```bash
./helm/scripts/create-user-singlenode.sh
```

The user script creates the server-side account only. It does not create a
`.fastdb.ini` file or a Kubernetes client Secret. Each application that uses
the FASTDB Python client is responsible for supplying its own credentials; a
Kubernetes application will typically create and mount a Secret containing its
`.fastdb.ini` file in that application's namespace.

The installer takes all Helm image settings from the selected values file. It
uses the configured shell image to prepare the host-mounted `install/` tree,
but it does not build deployment images. Kubernetes pulls remote registry
images when needed; locally built images must first be loaded with
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

### Registry Credentials

Deployments that pull from private registries (e.g., GHCR) need a
`dockerconfigjson` secret. The Helm chart creates this automatically when
`global.registryCredentials.enabled` is `true` in your values file. Pass the
password with `--set` so it is not committed to git.

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

Then pass the password to Helm:

```bash
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

When FASTDB is served from a subdirectory (e.g., `https://host/fastdb-app/`
instead of `https://host/`), the frontend JavaScript and HTML templates must
be built with the correct base path. The Automake build system uses
`@external_url@` placeholders in `.js.in` and `.html.in` files that get
substituted during `./configure`:

```
# Template (fastdb.js.in):
import { rkWebUtil } from "@external_url@static/rkwebutil.js";

# Built with --with-external-url=/fastdb-app/ → (fastdb.js):
import { rkWebUtil } from "/fastdb-app/static/rkwebutil.js";

# Built without (default) → (fastdb.js):
import { rkWebUtil } from "static/rkwebutil.js";
```

The single-node installer accepts the public URL through
`FASTDB_EXTERNAL_URL`:

```bash
FASTDB_EXTERNAL_URL=https://fastdb.example.org/ \
  ./helm/scripts/install-singlenode-fastdb.sh \
  ./helm/fastdb/values-arbutus.yaml
```

The `--external-url` value must end in a trailing slash.  It can be either a
path (such as `/fastdb-app/`) or a complete public URL (such as
`https://fastdb.example.org/fastdb-app/`). Use the complete URL when
FASTDB needs to send an absolute link in email, including password-reset links.
For the local Kind helper, this is `http://localhost:8080/`.

Two settings work together:
- `FASTDB_EXTERNAL_URL=/path/` or `https://host/path/` → bakes the URL/path into
  static JS/HTML at build time and into links generated by the server
- `webap.basePath: /path` → sets `SCRIPT_NAME` env var for Flask routing at runtime

For a root-path deployment, omit `webap.basePath`. Pass the public root URL
through `FASTDB_EXTERNAL_URL` when generated emails must point to that address.

## Common Operations

### Install a New Release

```bash
helm upgrade --install <release-name> ./helm/fastdb \
  -f ./helm/fastdb/values-<env>.yaml -n <namespace> --create-namespace
```

> **Important:** Always pass `-n <namespace>` so the Helm release is stored in the correct namespace (not `default`). Use `--create-namespace` if the namespace may not exist yet.

### Upgrade an Existing Release

After modifying values or templates:

```bash
helm upgrade <release-name> ./helm/fastdb \
  -f ./helm/fastdb/values-<env>.yaml -n <namespace>
```

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

> **Warning:** Deleting the namespace as well as the release can delete its
> registry secret and PVCs. Data stored in deleted PVCs may be lost.

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
  -f ./helm/fastdb/values-myenv.yaml \
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
| `values-arbutus.yaml` | Single-node VM using registry images |
| `values-arbutus-dev.yaml` | Single-node VM using locally built images |
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
| `templates/wal-webserver.yaml` | Optional WAL archive webserver |
