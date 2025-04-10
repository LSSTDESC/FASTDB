* FASTDB
  * [Overview](#overview)
  * [Using the FASTDB client](#using-the-fastdb-client)
* [Rob Notes](#rob-notes)
  * [Installing fastdb_rknop_dev](#installing-fastdb_rknop_dev)
  * [Building for tests](#building-for-tests)

# FASTDB
Development of the Fast Access to Survey Transients Database (FASTDB).

## Overview

FASTDB runs with two database backends, a PostgreSQL server and a Mongodb server.  Neither database server is directly accessible; rather, you access FASTDB through a webserver.  As of this writing, only one instance of FASTDB exists at https://fastdb-rknop-dev exists; that is Rob's development server, so its state is always subject to radical change.

While there will be an interactive UI on the webserver, the primary way you connect to FASTDB is using the web API.  To simplify this, there is a [python client library](#using-the-fastdb-client) that handles logging in and sending requests to the web server.  As of this writing, the only web API endpoints defined are ones that allow you to send raw SQL to the PostgreSQL web server.  (It's a readonly connection, so you can only read the database, not modify it.)

To use a FASTDB instance, you must have an account on it.  Contact Rob to ask for an account; he will need the username you want, and the email you want associated with it.  When first created, your account will no thave a password.  Point your web browser at the webserver's URL, and you will see an option to request a password reset link.


## Using the FASTDB Client

See the instructions and example at <a href="examples/using_fastdb_client.ipynb">using_fastdb_client.ipynb</a>. (That's a Jupyter notebook that you can copy and try running yourself.)

# Rob Notes

This README file needs to be orgnized better.

## Installing fastdb_rknop_dev

The base directory is
```
cd /global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev
```


In that directory there should be subdirectories `install`, `query_results`, and `sessions`.  There should also be a `FASTDB` directory which was generated with
```
git clone git@github.com:LSSTDESC/FASTDB
cd FASTDB
git checkout <version>
git submodule update --init
```

(With `git pull` updates and such as usual.)

### Updating the installed software

Do this before first installation of the workloads, and then again later as necessary as code is modified.

To rebuild, do the following.  The "touch" steps are necessary because a `git clone` or `git pull` doesn't preserve timestamps, so autotools make think stuff needs to be rebuilt that doesn't.  If you've actually changed `configure.ac` or one of the `Makefile.am` files, make sure that you've done the necessary `autoreconf` steps, otherwise this will probably just break everything.

```
cd /global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev/FASTDB
touch aclocal.m4 configure
find . -name Makefile.am -exec touch \{\} \;
find . -name Makefile.in -exec touch \{\} \;
./configure \
  --with-installdir=/global/cfs/cdirs/lsst/groups/TD/SOFTWARE/fastdb_deployment/rknop_dev/install \
  --with-smtp-server=smtp.lbl.gov \
  --with-smtp-port=25 \
  --with-email-from=raknop@lbl.gov
make install
```

If the server is already running, remember to `KILL -HUP 1` on the webserver workload.  Then look at the logs and see if it crashed, and start the nightmare of iterative editing and restarting the workload to try to make it go again.

### Migrating the database

Upon initial installation, and after any database schema change, log into the spin workload running the shell and run:
```
cd /code/db
python apply_migrations.py -H <host> -u <dbuser> -p <dbpasswd>
```

(On initial install, probably need to do this before starting the query runner or query pruner.)


### Creating a user:

Upon first install, there's no user that can log into the web ap.  On the shell, run
```
psql -h postgres -U postgres fastdb
```
giving it the postgres password, and run:
```
INSERT INTO authuser(username,displayname,email) VALUES ('rknop','Rob Knop','raknop@lbl.gov')
```

Then go and reset your password using the web/email interface.

### Installing spin workloads

Work in rancher context `m1727`, namespace `fastdb-rknop-dev`.

Look under `FASTDB/admin/spin/rknop_dev` to find the Spin yaml files.

Docker images to build (using `DOCKER_BUILDKIT=1`) and push:

* `FASTDB/docker/postgres/Dockerfile` : build and push the image referred to in `postgres.yaml`
* `FASTDB/docker/mongodb/Dockerfile` : build and push the image referred to in `mongodb.yaml`
* `FASTDB/docker/webserver/Dockerfile` : build and push the two images referred to in `webserver.yaml` (using `--target webap`) and `shell.yaml` (using `--target shell`).
* `FASTDB/docker/query_runner/Dockerfile` : build and push the image referred to in `query-runner.yaml`

The relevant `yaml` files to apply (more or less in order) are:

* `secrets.yaml` : *important* make sure not to commit any actual secrets to the git archive.  Before applying, edit this file to put in the actual secrets.  A file with the actual secrets is in `~/secrets/fastdb-rknop-dev-secrets.yaml`.
* `webserver-cert.yaml` : likewise, do not commit the actual cert info to the git archive.  File with actual secrets is in `~/secrets/fastdb-rknop-dev-webserver-cert.yaml`.
* `postgres-pvc.yaml` : volume claim for PostgreSQL
* `mongodb-pvc.yaml` : volume claim for MongoDB
* `postgres.yaml` : runs the PostgreSQL database
* `mongodb.yaml` : runs the MongoDB database
* `webserver.yaml` : runs the webserver at `fastdb-rknop-dev.lbl.gov`
* `query-runner.yaml` : runs the server that handles the long SQL query interface
* `query-pruner.yaml` : runs the cron job that cleans up old long queries
* `shell.yaml` : runs an interactive shell; may not usually be necessary


## Building for tests

This is building for tests to be run on your local machine using the `docker-compose.yaml` file in the root of the checkout.

If you've edited `configure.ac` or any of the `Makefile.am` files, run
```
autoreconf --install
```

Then, run

```
./configure --with-installdir=$PWD/install \
            --with-smtp-server=mailhog \
            --with-smtp-port=1025
make install
```

Should put lots of stuff underneath `install`.

To run tests, remember to docker compose up all of `shell`, `webap`, and `query-runner`.


