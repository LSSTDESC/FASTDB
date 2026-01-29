# FASTDB Helm Chart Guide

This guide explains how to deploy FASTDB to Kubernetes clusters using Helm.

## Conceptual Overview

### What is Helm?

Helm is a package manager for Kubernetes. It uses **charts** (packages of pre-configured Kubernetes resources) to deploy applications. Think of it like `apt` or `brew` but for Kubernetes.

### How FASTDB Helm Chart Works

```
helm/fastdb/
├── Chart.yaml              # Chart metadata (name, version)
├── values.yaml             # Default configuration values
├── values-local.yaml       # Local Kind cluster overrides
├── values-slac.yaml        # SLAC S3DF overrides
└── templates/              # Kubernetes manifest templates
    ├── _helpers.tpl        # Reusable template functions
    ├── namespace.yaml
    ├── secrets.yaml
    ├── pvcs.yaml
    ├── postgres.yaml
    ├── mongodb.yaml
    ├── webap.yaml
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
- Docker images built and accessible

### Deploy to Local Kind Cluster

```bash
# 1. Create Kind cluster with port mappings
kind create cluster --name fastdb-local --config admin/local/kind-config.yaml

# 2. Build and load images
docker-compose build postgres mongodb shell webap queryrunner
docker tag ghcr.io/lsstdesc/fastdb-postgres:test20251201 fastdb-postgres:local
docker tag ghcr.io/lsstdesc/fastdb-mongodb:test20251201 fastdb-mongodb:local
docker tag ghcr.io/lsstdesc/fastdb-shell:test20251201 fastdb-shell:local
docker tag ghcr.io/lsstdesc/fastdb-webap:test20251201 fastdb-webap:local
docker tag ghcr.io/lsstdesc/fastdb-query-runner:test20251201 fastdb-query-runner:local

kind load docker-image fastdb-postgres:local fastdb-mongodb:local \
  fastdb-shell:local fastdb-webap:local fastdb-query-runner:local \
  --name fastdb-local

# 3. Deploy with Helm
helm install fastdb ./helm/fastdb -f ./helm/fastdb/values-local.yaml

# 4. Verify
kubectl get pods -n fastdb-local
curl http://localhost:8080
```

### Deploy to SLAC S3DF

```bash
# Ensure kubectl context points to SLAC cluster
kubectl config use-context your-slac-context

# Deploy with namespace override
helm install fastdb ./helm/fastdb \
  -f ./helm/fastdb/values-slac.yaml \
  --set global.namespace=your-namespace \
  --set global.namespaceLabels.owner=your-username
```

## Common Operations

### Install a New Release

```bash
helm install <release-name> ./helm/fastdb -f ./helm/fastdb/values-<env>.yaml
```

### Upgrade an Existing Release

After modifying values or templates:

```bash
helm upgrade <release-name> ./helm/fastdb -f ./helm/fastdb/values-<env>.yaml
```

### View Current Values

```bash
helm get values <release-name>
```

### Preview Changes (Dry Run)

See what would be deployed without actually deploying:

```bash
helm template <release-name> ./helm/fastdb -f ./helm/fastdb/values-<env>.yaml
```

Or with diff against current deployment:

```bash
helm upgrade <release-name> ./helm/fastdb -f ./helm/fastdb/values-<env>.yaml --dry-run
```

### Uninstall a Release

```bash
helm uninstall <release-name>
```

Note: This removes deployments but PVCs may persist. Delete manually if needed:

```bash
kubectl delete pvc -n <namespace> --all
```

### List Releases

```bash
helm list
helm list -n <namespace>
```

### Rollback to Previous Version

```bash
helm rollback <release-name> <revision-number>
helm history <release-name>  # View revision history
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
helm install fastdb ./helm/fastdb -f ./helm/fastdb/values-myenv.yaml
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
