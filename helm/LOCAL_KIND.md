# Local Kind development

This optional workflow runs FASTDB on a laptop using Kind. For a single-VM
benchmark deployment, follow the [main guide](README.md).

Install Docker, Docker Compose, Kind, Helm and kubectl. From the FASTDB
repository root, initialize submodules and run:

```bash
git submodule update --init --recursive
./helm/scripts/create-local-cluster.sh
./helm/scripts/install-local-fastdb.sh
kubectl --context kind-fastdb-local get pods,pvc -n fastdb-local
```

The first script creates the cluster. The second builds and loads the images,
generates `install/`, and installs the chart using
[values-local.yaml](fastdb/values-local.yaml). It uses a different workflow and
image configuration from the single-VM installer.

Rerun `install-local-fastdb.sh` after changing application code, migrations,
images or chart settings; it updates the existing installation without
deliberately deleting database storage. Verify the configured image tags match
the images actually built by Compose if you change its defaults.

For local account creation, use:

```bash
./helm/scripts/create-user-local.sh
```

This is the browser/MailHog password-reset workflow described in
[HOWTO_LOGIN.md](../HOWTO_LOGIN.md), rather than the direct user-creation helper
used on a VM.

To delete the Kind cluster and all FASTDB data stored within it:

```bash
./helm/scripts/stop-local.sh
```

This is a destructive reset, not a pause. The next cluster creation starts a
fresh environment.
