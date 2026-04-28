# ADR: Deploy Script with kubectl cp for Helm Code Delivery

**Status:** Accepted
**Date:** 2026-01-29
**Decision:** Use a deploy script that copies build artifacts to a shared PVC via `kubectl exec` + tar pipe, rather than baking code into images or using Helm hooks

## Context

FASTDB's Helm deployment needs application code (Python, SQL migrations, config) available at `/fastdb` inside pods. The `install/` directory is a build artifact produced by Automake (`./configure && make install` via `docker-compose run makeinstall`) — it is not in git and not baked into Docker images.

After `helm install`, pods that mount the code PVC crash because the PVC is empty. We need a mechanism to populate the code PVC as part of the deploy workflow.

### Constraints

- `install/` is a build artifact, not in version control
- SLAC S3DF vclusters do not support hostPath mounts (unlike local Kind clusters)
- The code PVC (`ReadWriteMany`) is shared across webap, queryrunner, shell, and createdb pods
- Docker images are generic base images (Python runtime, PostgreSQL client, etc.) — code is mounted in, not copied during build
- The shell pod runs `sleep infinity` with no code dependency, making it available immediately after deploy

## Decision

**Use a deploy script (`scripts/helm-deploy.sh`) that orchestrates the full deploy cycle:**

1. Build `install/` via `docker-compose run makeinstall`
2. Run `helm upgrade --install` to create/update all Kubernetes resources
3. Wait for the shell pod to be ready
4. Copy `install/` and `db/` contents to the code PVC via tar pipe through the shell pod
5. Restart webap and queryrunner deployments to pick up the new code

Code is copied using tar pipes for reliable content-level transfer:

```bash
tar cf - -C install . | kubectl exec -i -n $NS $SHELL_POD -- tar xf - -C /fastdb/
kubectl exec -n $NS $SHELL_POD -- mkdir -p /fastdb/db
tar cf - -C db . | kubectl exec -i -n $NS $SHELL_POD -- tar xf - -C /fastdb/db/
```

## Rationale

### 1. No Template Changes Required

The existing Helm templates already mount the code PVC at `/fastdb`. The shell pod runs `sleep infinity` with no startup dependency on code being present. No init containers, Helm hooks, or sidecar changes are needed.

### 2. Shell Pod is the Natural Entry Point

The shell deployment is already part of the chart for debugging. It starts immediately (no code dependency), mounts the code PVC read-write, and is always available. Using it as the copy target is a zero-cost approach.

### 3. Tar Pipe is Reliable

`kubectl cp` uses tar under the hood but has known issues with symlinks and permissions. Using explicit `tar cf - | kubectl exec -i -- tar xf -` gives direct control over what gets copied and where, handles all file types correctly, and streams without intermediate files.

### 4. createdb Job Self-Heals

The createdb Job uses `restartPolicy: OnFailure`. If it starts before code is on the PVC, it fails and retries automatically. Once the script copies code, the next retry succeeds. No ordering dependency needs to be encoded in Helm.

### 5. Separating Build from Deploy is Correct

The build step (`docker-compose run makeinstall`) runs Automake in a container with the full toolchain. The deploy step copies the result. This separation means:

- Builds are reproducible (same container, same toolchain)
- Deploy doesn't need the build toolchain
- `--skip-build` allows redeploying the same code (e.g., after a config change)
- `--skip-helm` allows updating just the code without touching Kubernetes resources

## Alternatives Considered

### Alternative 1: Bake Code into Docker Images

Build `install/` during `docker build` so images contain the code.

**Rejected because:**
- The current architecture deliberately separates runtime images from application code
- Every code change would require rebuilding and pushing all images (webap, queryrunner, shell, createdb)
- Image sizes would increase significantly
- Local development workflow uses host mounts for fast iteration — baking code into images would break this pattern
- Would require changing the existing `docker-compose` build structure

### Alternative 2: Helm Hook (pre-install/post-install Job)

Use a Helm hook Job that runs `kubectl cp` or pulls code from a git repo.

**Rejected because:**
- Hook Jobs can't access the host filesystem to copy local build artifacts
- A git-clone hook would need git credentials in the cluster and wouldn't have the Automake build step
- Hook ordering with PVC creation is fragile
- Adds template complexity for something that's better handled outside Helm

### Alternative 3: Init Container with Git Clone

Add an init container to webap/queryrunner that clones the repo and runs `make install`.

**Rejected because:**
- Requires git credentials in the cluster
- Requires the full Automake toolchain in the init container image
- Dramatically increases pod startup time (clone + configure + make)
- Network dependency at pod startup (git clone can fail)
- Every pod restart rebuilds from source
- `install/` should be built once, not per-pod

### Alternative 4: S3/Object Storage Artifact

Upload `install/` to S3, download in init container.

**Rejected because:**
- Adds infrastructure dependency (S3 bucket, credentials)
- Over-engineered for a development/small-team deployment
- Still needs an init container or sidecar
- SLAC S3DF may not have S3 access from vclusters

### Alternative 5: hostPath Volume (Kind-only approach)

Mount the host filesystem directly into pods.

**Rejected because:**
- Only works with Kind (local development)
- SLAC S3DF vclusters do not allow hostPath mounts
- Not portable across deployment environments
- Already used as the Kind-specific path (`volumes.codeHostPath: true`)

## Trade-off Analysis

| Concern | Deploy Script (chosen) | Bake into Images | Init Container | Helm Hook |
|---------|----------------------|-------------------|----------------|-----------|
| **Template changes** | None | Major | Moderate | Moderate |
| **Build/deploy separation** | Clean | Coupled | Coupled | Partial |
| **Pod startup time** | Normal | Normal | Slow (build) | Normal |
| **Network dependency** | kubectl only | Registry | Git/network | Varies |
| **Code update speed** | Fast (copy) | Slow (rebuild all images) | Slow (rebuild) | Moderate |
| **Complexity** | One shell script | Dockerfile changes | Init container config | Hook job config |
| **Works in vcluster** | Yes | Yes | Yes | Yes |

## Consequences

### Positive

- Zero changes to existing Helm templates
- Fast code updates (`--skip-build --skip-helm` for code-only redeploy)
- Clear separation between build and deploy steps
- Works in all target environments (Kind, SLAC S3DF, NERSC SPIN)
- Script is self-documenting with `--help`

### Negative

- Requires running the script (not pure `helm install`)
- Code is not part of the Helm release — `helm rollback` doesn't roll back code
- Brief window after `helm install` where pods have no code (createdb retries; webap/queryrunner restart after copy)
- Depends on shell pod being enabled and healthy

### Mitigations

1. **Script is the documented deploy path** — `HELM_HOWTO.md` references the script as the primary method
2. **createdb retries automatically** — `restartPolicy: OnFailure` handles the timing gap
3. **webap/queryrunner restart after copy** — ensures they always start with fresh code
4. **`--skip-build` and `--skip-helm` flags** — allow partial runs for specific scenarios

## Usage

```bash
# Full deploy (build + helm + copy + restart)
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml

# Code-only update (skip build and helm, just copy and restart)
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml --skip-build --skip-helm

# Config change (skip build, run helm upgrade, copy, restart)
./scripts/helm-deploy.sh ccosta-dev ./helm/fastdb/values-ccosta-dev.yaml --skip-build
```

## When to Reconsider

Reconsider this decision if:

1. **Images are rebuilt to include code** — If the team decides to bake code into images, the copy step becomes unnecessary
2. **CI/CD pipeline is added** — A pipeline could build images with code baked in, making the script unnecessary for automated deploys
3. **Code PVC is replaced** — If the architecture moves away from shared PVCs for code delivery
4. **Team grows significantly** — Larger teams may need more formal artifact management (container registry, artifact storage)
