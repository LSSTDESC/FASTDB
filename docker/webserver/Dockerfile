#
# DOCKER_BUILDKIT=1 docker build --target <target> -t <image>:<label>.
#
# You probably want to build for both targets webserver and shell
#
# Rob, use:
#   docker build --target webserver -t registry.nersc.gov/m1727/raknop/fastdb-webap:...
#   docker build --target shell -t registry.nersc.gov/m1727/raknop/fastdb-shell:...

FROM debian:trixie-20260223 AS base
LABEL maintainer="Rob Knop <rknop@pobox.com>"

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

RUN  apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install -y \
         python3 locales netcat-openbsd gnupg curl elinks postgresql-client make rlwrap socat \
         ca-certificates \
         tmux emacs-nox less procps bzip2 \
    && apt-get -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN cat /etc/locale.gen | perl -pe 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' > /etc/locale.gen.new \
    && mv /etc/locale.gen.new /etc/locale.gen
RUN locale-gen en_US.utf8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

RUN ln -s /usr/bin/python3 /usr/bin/python
ENV LESS=-XLRi


# Install mongosh (which adds >300MB to the docker image :( ) and mongodb-dbatabase-tools (another 150MB)

RUN curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

# Installing the bookworm mongosh because last I checked, there wasn't a mongodb-database-tools
#    package available for trixie
RUN echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] http://repo.mongodb.org/apt/debian trixie/mongodb-org/8.0 main" | tee /etc/apt/sources.list.d/mongodb-org-8.0.list

RUN apt-get update \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
          mongodb-mongosh \
 && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# ...mongodb-database-tools isn't always in the archive I just added!  It's missing for
#   trixie, and even for bookworm it's missing for arm64.  This mongo
#   archive is kind of a disaster.  Installation of it has been moved to build.


# ======================================================================
FROM base AS build

WORKDIR /usr/src

RUN DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get update \
    && DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get -y install -y python3-pip python3-venv git libpq-dev golang

RUN mkdir /venv
RUN python3 -mvenv /venv

RUN source /venv/bin/activate && \
    pip --no-cache install \
       apscheduler==3.11.2 \
       astropy==7.2.0 \
       confluent_kafka==2.13.2 \
       "fastavro>=1.10.0,<2.0.0" \
       fink-client==10.1 \
       flask==3.1.3 \
       flask-session==0.8.0 \
       flask-sock==0.7.0 \
       gevent==25.9.1 \
       gunicorn==25.1.0 \
       nested-pandas==0.6.8 \
       numpy==2.4.3 \
       pandas==2.3.3 \
       pittgoogle-client==0.3.19 \
       polars==1.38.1 \
       psycopg==3.3.3 \
       pycryptodome==3.23.0 \
       pymongo==4.16.0 \
       pytest==9.0.2 \
       pytest-timestamper==0.0.10 \
       python-dateutil==2.9.0.post0 \
       pytz==2026.1.post1 \
       remote-pdb==2.1.0 \
       requests==2.32.5 \
       simplejson==3.20.2

# Our own customization of pubsub
COPY pubsub.patch /tmp/pubsub.patch
RUN patch /venv/lib/python3.13/site-packages/pittgoogle/pubsub.py /tmp/pubsub.patch

# Irritating that the mongodb debian apt aarchive doesn't have debian
#   arm packages for mongodb-database-tools, nor does it have them for
#   any architecture for trixie.  So, build from source.
#
# (...and even though this needs golang installed for ./make build to even run,
#  the first thing it does it download go...)
RUN git clone https://github.com/mongodb/mongo-tools.git \
  && cd mongo-tools \
  && ./make build \
  && cp -p bin/mongodump /usr/local/bin \
  && cp -p bin/mongorestore /usr/local/bin \
  && cd .. \
  && rm -rf mongo-tools

# ======================================================================
# This is for the test webserver.  It installs crappy keys so you
#   can connect via https.
# The production webserver is going to be on Spin and won't do
#   SSL management itself (the spin ingress handles that).
FROM base AS test

COPY --from=build /venv/ /venv/
COPY --from=build /usr/local/bin/mongodump /usr/local/bin/mongodump
COPY --from=build /usr/local/bin/mongorestore /usr/local/bin/mongorestore
ENV PATH=/venv/bin:$PATH

RUN mkdir -p /fastdb/webserver
ENV PYTHONPATH=/fastdb
WORKDIR /fastdb/webserver

RUN mkdir /sessions

COPY docker_flask/createdb.py /usr/src/createdb.py
COPY docker_flask/key.pem /usr/src/key.pem
COPY docker_flask/cert.pem /usr/src/cert.pem

EXPOSE 8080
ENTRYPOINT [ "gunicorn", "--certfile", "/usr/src/cert.pem", "--keyfile", "/usr/src/key.pem", \
           "-w", "1", "--threads", "10", "-b", "0.0.0.0:8080", "--timeout", "0", "server:app" ]

# ======================================================================
# This is for interaction.  It runs something that will stay up
#   that you can exec a shell on.
FROM base AS shell

COPY --from=build /venv/ /venv/
COPY --from=build /usr/local/bin/mongodump /usr/local/bin/mongodump
COPY --from=build /usr/local/bin/mongorestore /usr/local/bin/mongorestore
ENV PATH=/venv/bin:$PATH

RUN mkdir -p /fastdb/webserver
ENV PYTHONPATH=/fastdb
WORKDIR /fastdb

ENTRYPOINT [ "tail", "-f", "/etc/issue" ]

# ======================================================================
# This is for the production webserver
FROM base AS webserver

COPY --from=build /venv/ /venv/
COPY --from=build /usr/local/bin/mongodump /usr/local/bin/mongodump
COPY --from=build /usr/local/bin/mongorestore /usr/local/bin/mongorestore
ENV PATH=/venv/bin:$PATH

RUN mkdir -p /fastdb/webserver
ENV PYTHONPATH=/fastdb
WORKDIR /fastdb/webserver

RUN mkdir /sessions

EXPOSE 8080
# ENTRYPOINT [ "/venv/bin/gunicorn", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "40", "server:app" ]
ENTRYPOINT [ "/venv/bin/gunicorn", "-b", "0.0.0.0:8080", "-k", "gevent", "--timeout", "300", "--workers", "2", "server:app" ]
