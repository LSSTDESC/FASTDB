# Deploy FASTDB on a single VM

This guide is for a maintainer setting up a reproducible FASTDB development or
benchmark installation on one Linux VM. It covers cluster prerequisites,
installation, a test user, simulated LASS alerts, and verification through the
FASTDB Python client on the VM. It is not a full production operations guide.

Run the commands on the VM as a normal user with Docker and Kubernetes access.
Commands assume the FASTDB repository is your current directory unless stated
otherwise. External access and browser access are outside this guide's scope.

## Contents

- [Prepare the VM](#1-prepare-the-vm)
- [Choose deployment settings](#2-choose-deployment-settings)
- [Install FASTDB](#3-install-fastdb)
- [Verify the deployment](#4-verify-the-deployment)
- [Create a user and query FASTDB](#5-create-a-user-and-query-fastdb)
- [Ingest simulated LASS alerts](#6-ingest-simulated-lass-alerts)
- [Update an installation](#update-an-installation)
- [Troubleshooting](#troubleshooting)
- [Reference and other workflows](#reference-and-other-workflows)

## 1. Prepare the VM

Install a single-node Kubernetes distribution, such as K3s, and configure its
default storage provisioner to put database files on your chosen disk or mounted
filesystem. Prepare and mount that storage before deploying FASTDB. A PVC requests
storage from the cluster; it does not create a separate cloud disk or a backup.

Install Git, Docker Engine with Docker Compose, Helm, and kubectl. Confirm that
your normal user can run:

```bash
docker info
docker compose version
kubectl get nodes
kubectl get storageclass
helm version
df -h /
```

`kubectl get nodes` should show `Ready`, and a usable storage class should be available.
Allow space for Docker images, Kubernetes images, database data, logs, and LASS's
Kafka data. The example PVC sizes are requests, not preallocated spare disk space;
monitor the underlying filesystem as data grows.

For a worked Ubuntu/K3s setup, review
[scripts/setup-arbutus-k3s.sh](scripts/setup-arbutus-k3s.sh). It installs the tools
and configures the user's Kubernetes access. It is an infrastructure-specific
example designed for the DRAC arbutus-cloud: inspect its version pins and `DEFAULT_LOCAL_STORAGE_PATH` before using
it. The current path is `/fastdb/single-staging`, assuming CephFS is already
mounted at `/fastdb`. It does not mount CephFS or change an existing K3s
installation's storage path.

For cluster setup details, use the official
[K3s installation guide](https://docs.k3s.io/quick-start),
[K3s storage guide](https://docs.k3s.io/add-ons/storage), and
[Kubernetes storage documentation](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).

Clone FASTDB and select the branch or release you intend to test:

```bash
git clone --recurse-submodules https://github.com/CanDIAPL/FASTDB.git
cd FASTDB
git checkout <deployment-branch-or-release>
git submodule update --init --recursive
```

Optional: You can record the commit used for a benchmark with `git rev-parse HEAD`.

## 2. Choose deployment settings

Either use one of the existing values files or copy the registry-based single-VM example into your own values file:

```bash
cp helm/fastdb/values-arbutus.yaml helm/fastdb/values-my-vm.yaml
```

This example describes a single-VM deployment. Review these
settings before installing:

```yaml
global:
  namespace: fastdb-arbutus
  imageRegistry: images.canfar.net/candiapl
  imageTag: manual-20260918
  imagePullPolicy: IfNotPresent

volumes:
  type: pvc
  storageClass: local-path
  codeHostPath: true
```

The installer reads the registry and tag from this file. Change `imageTag` to
use another published image release; keep the FASTDB images on a compatible tag.
The example images were built for AMD64, so use an AMD64 VM for this workflow.

Set `storageClass` to the class configured in step 1, or use `storageClass: ""`
to request the cluster's default class. Review PostgreSQL, MongoDB, logs, sessions,
and query-result sizes in the example. PostgreSQL standby is disabled.

Keep the example's LASS ingestion settings for the walkthrough below: topic
`lass-topic`, schema v10.0, collection base `lass`, and processing version
`lass`. The installer discovers the node's internal IP for the consumer's
connection to a LASS broker on the same VM. Other broker configurations are possible but 
not described in this walkthrough.

For subsequent commands, set the namespace to match your values file:

```bash
export FASTDB_NAMESPACE=fastdb-arbutus
```

The installer reads its namespace from the values file, whereas the user helper
reads `FASTDB_NAMESPACE`. Keep them consistent.

If registry authentication is needed, run:

```bash
docker login images.canfar.net
```

For Kubernetes to use that login as well, enable the following in your values
file (uncomment it in the supplied example):

```yaml
global:
  imagePullSecrets:
    - name: fastdb-registry
```

Add this under the existing `global:` section. The installer copies the Docker
login file into a Kubernetes pull Secret named `fastdb-registry`. Docker and
Kubernetes have separate image stores and authentication settings; a successful
Docker pull alone does not configure Kubernetes.

## 3. Install FASTDB

From the repository root:

```bash
./helm/scripts/install-singlenode-fastdb.sh helm/fastdb/values-my-vm.yaml
```

If the shell image is missing from Docker, the script explains why it is needed
and asks before pulling it. The script then:

1. Generates database passwords and an application secret key on the first run.
2. Runs the temporary Compose `makeinstall` service to configure and copy the
   current checkout into `install/`.
3. Runs Helm to create the database storage, services, migration Job, and
   ingestion workloads. Kubernetes obtains the deployment images.
4. Restarts webap, queryrunner, and shell to load the generated code, and checks
   the local web service.

Keep the generated `helm/fastdb/values-arbutus-secrets.yaml` file for upgrades.
It contains plaintext deployment secrets, is ignored by Git, and is created with
permissions restricted to the owner. It is distinct from a FASTDB user's password.
Do not commit it or replace it while reusing existing databases.

The current application code comes from your checkout, even when using older
registry images. The images supply the runtime dependencies. Changes requiring
new dependencies may require a newer image tag.

A successful run ends with Helm reporting `STATUS: deployed`, completed
rollouts, and the final workload/storage listing. Startup can take several
minutes on a small VM. Kubernetes continues working if Helm times out; inspect
its state before starting another installation.

## 4. Verify the deployment

```bash
helm status fastdb -n "$FASTDB_NAMESPACE"
kubectl get pods,pvc -n "$FASTDB_NAMESPACE"
kubectl get jobs -n "$FASTDB_NAMESPACE"
```

Expect:

- All PVCs `Bound`.
- PostgreSQL, MongoDB, webap, queryrunner, shell, MailHog and brokerconsumer ready.
- `createdb` and `ingestion-processing-version` Jobs completed.
- Periodic source-importer Jobs; a successful finished pod shows `Completed`.

Brokerconsumer normally shows `2/2`: one consumer container and one Kafka
connection proxy. It is one pod with two containers.

Inspect the consumer and scheduled importer:

```bash
kubectl logs deployment/brokerconsumer -c brokerconsumer \
  -n "$FASTDB_NAMESPACE" --tail=100
kubectl get cronjob source-importer -n "$FASTDB_NAMESPACE"
kubectl get pods -n "$FASTDB_NAMESPACE" --sort-by=.metadata.creationTimestamp
```

Use the latest source-importer pod name from that list:

```bash
kubectl logs <source-importer-pod-name> \
  -n "$FASTDB_NAMESPACE" --timestamps --tail=100
```

Before LASS starts, Kafka connection/topic warnings are expected. An
empty-collection `NoneType` error in source importer is a known application
issue on checkouts lacking the empty-collection fix; it does not by itself mean
database installation failed.

## 5. Create a user and query FASTDB

Create a test account from the VM:

```bash
FASTDB_NAMESPACE="$FASTDB_NAMESPACE" ./helm/scripts/create-user-singlenode.sh
```

The helper prompts for account details and a password, generates authentication
keys, and inserts the account into PostgreSQL. It does not require MailHog or
create a client configuration file. See [user creation details](../HOWTO_LOGIN.md).

You can test your login using the web app or the python client (NOTE: documentation on how to do this to come soon)

## 6. Ingest simulated LASS alerts

Run LASS on the same VM as FASTDB. Use a separate virtual environment with a
Python version supported by the selected LASS release:

```bash
python3.13 -m venv "$HOME/lass-venv"
source "$HOME/lass-venv/bin/activate"
python -m pip install lass
lass --help
lass -n 100 --avro
```

Optional: Record the LASS version with `python -m pip show lass` when benchmarking.

The current [LASS CLI](https://github.com/CanDIAPL/lass#usage) starts Docker
container `lassbroker`, publishes Kafka on VM port 19092, and sends to
`lass-topic`. Use schema 10 and `--avro` to match the chart's v10.0 schemaless
Avro consumer. LASS defaults to pickle, which does not match that consumer.

Verify the broker and follow the consumer:

```bash
docker ps --filter name=lassbroker
kubectl logs -f deployment/brokerconsumer -c brokerconsumer \
  -n "$FASTDB_NAMESPACE" --tail=50
```

Stop following logs with Ctrl-C. The consumer writes alerts to MongoDB; the
source-importer CronJob transfers them to PostgreSQL, on a schedule configured in the 
values file. Default in the dev values files is once per minute, production is expected 
to be closer to once per hour. 
Check the latest importer logs as in step 4 for imported object/source counts.

## 7. Check everthing worked with the python client

To check everything worked correctly, open the FASTDB shell and use the Python client to query the db.

Enter the FASTDB shell pod from the VM:

```bash
kubectl exec -it deployment/shell -n "$FASTDB_NAMESPACE" -- /bin/bash
```

Inside that shell, start Python:

```bash
python
```

Use the Python client to authenticate and run a query through the web API:

```python
from getpass import getpass
from fastdb.fastdb_client import FASTDBClient

fdb = FASTDBClient(
    "http://webap:8080/",
    username=input("FASTDB username: "),
    password=getpass("FASTDB password: "),
)

#check if you are connected to the DB
print(fdb.submit_short_sql_query("SELECT 1 AS connected"))

#count the number of entries in the diasource table
print(fdb.submit_short_sql_query("SELECT COUNT(*) AS sources FROM diasource"))

#print the IDs and times for the first new diasources 
print(fdb.submit_short_sql_query(
    "SELECT diasourceid, diaobjectid, midpointmjdtai FROM diasource LIMIT 5"
))
```

Use `exit()` to leave Python, then `exit` to leave the pod shell.

After import succeeds, expect a nonzero source count and example rows. Do not
assume the total equals the number just sent if the database already contains data.
Start with this small run before increasing alert counts, and monitor free space
on both the database filesystem and the filesystem holding Docker/Kafka data.

This completes the basic check: LASS → Kafka → brokerconsumer → MongoDB →
source importer → PostgreSQL → authenticated FASTDB client.

## Update an installation

Keep the same namespace, values file, checkout location, and generated secrets.
After selecting the next code revision, rerun:

```bash
./helm/scripts/install-singlenode-fastdb.sh helm/fastdb/values-my-vm.yaml
```

The installer regenerates `install/`, recreates the one-shot setup Jobs, applies
migrations and Helm changes, and restarts services. It should preserve existing
database PVCs, it is not a database reset. Review migrations and back up useful
data before an update. Changing the namespace creates a different deployment.
An image-tag change and a source-code change are separate choices.

## Troubleshooting

AI-generated tips on troubleshooting issues with the fastdb deployment:

Collect evidence before deleting resources:

```bash
helm status fastdb -n "$FASTDB_NAMESPACE"
kubectl get pods,pvc,jobs -n "$FASTDB_NAMESPACE"
kubectl get events -n "$FASTDB_NAMESPACE" --sort-by=.metadata.creationTimestamp
kubectl logs job/createdb -n "$FASTDB_NAMESPACE" --all-containers=true
kubectl logs job/ingestion-processing-version -n "$FASTDB_NAMESPACE"
```

For a particular failing pod:

```bash
kubectl describe pod <pod-name> -n "$FASTDB_NAMESPACE"
kubectl logs <pod-name> -n "$FASTDB_NAMESPACE" --all-containers=true --tail=100
kubectl logs <pod-name> -n "$FASTDB_NAMESPACE" --previous --tail=100
```

`--previous` reads the previous container attempt and only works if one exists.
Use `-c <container-name>` for a pod with multiple containers. Completed Jobs and
pods can be cleaned up, so collect their logs promptly.

| Symptom | What to inspect |
| --- | --- |
| PVC stays `Pending` | `kubectl describe pvc <claim> -n "$FASTDB_NAMESPACE"`; confirm the storage class, mount and free space. For K3s, inspect `kubectl logs -n kube-system deployment/local-path-provisioner`. |
| Helm times out | Check PVCs, pod events and setup Job logs. A failed Helm status can remain even if workloads later recover. |
| MongoDB repeatedly restarts | Inspect pod events for failed liveness checks and previous logs. Use a chart revision with the startup-probe fix. |
| Ingestion reports missing tables/columns | Confirm migrations completed before processing-version setup and importer execution. Do not repeatedly start ingestion against an incomplete schema. |
| Image pull fails | Verify registry, tag, architecture, credentials and Kubernetes pull Secret. |
| Cannot write `install/` | Check ownership of the checkout's `install/` and `db/` directories; use the maintainer account rather than running the installer as root. |

For VM resource diagnostics, use `df -h`, `free -h`, `uptime`,
`docker system df`, and `kubectl top nodes` (if metrics are available).
A PVC's requested capacity is not a measurement of disk consumption.

## Reference and other workflows

### What the installation uses

- [Chart defaults](fastdb/values.yaml) supply settings not overridden in your file.
- [Registry-image VM example](fastdb/values-arbutus.yaml) is the walkthrough's base.
- [Locally built image example](fastdb/values-arbutus-dev.yaml) uses `fastdb.local`.
- [Templates](fastdb/templates/) define Deployments, Jobs, Services, Secrets and PVCs.
- `install/` contains generated runtime files. Pods mount it at `/fastdb` using
  `hostPath`; migrations are mounted separately from `db/`. Keep the checkout
  in place. This is a single-node workflow.
- Database credentials are supplied through Kubernetes Secrets; user accounts
  are separate from those infrastructure credentials.

For tool details, see [Helm's usage guide](https://helm.sh/docs/intro/using_helm/)
and [Kubernetes workload documentation](https://kubernetes.io/docs/concepts/workloads/).

### Build images locally

For K3s, this helper builds images from the checkout and loads them into K3s:

```bash
./helm/scripts/build-local-images.sh fastdb.local test20260428
./helm/scripts/install-singlenode-fastdb.sh helm/fastdb/values-arbutus-dev.yaml
```

If you choose another registry/tag for the build, update the values file to match.
The image-loading helper specifically uses K3s; other distributions need their
own image import method or a reachable registry.

### Run the preparation and Helm steps manually

The installer is the recommended walkthrough because it also prepares secrets,
registry access, host paths and service restarts. Its two principal operations are
Compose `makeinstall` and `helm upgrade --install`. See
[scripts/install-singlenode-fastdb.sh](scripts/install-singlenode-fastdb.sh)
for the complete commands and overrides. Running bare `helm install` against
the example values does not generate the host's `install/` tree.

### Local laptop development

Use the separate [Kind guide](LOCAL_KIND.md).

### Removing a benchmark installation

Removing a Helm release and deleting its data are different operations.
Inspect the namespace's PVCs and the volumes' reclaim policies before uninstalling.
Deleting PVCs or the namespace can permanently delete the benchmark databases.
Do not use namespace deletion as an upgrade step. LASS's `lass --quit` removes
its Kafka container and simulated broker data independently of FASTDB.

After verification, keep a record of the FASTDB commit, image tag, values,
Kubernetes version and LASS version alongside benchmark results.
