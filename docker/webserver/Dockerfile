#
# DOCKER_BUILDKIT=1 docker build --target <target> -t <image>:<label>.
#
# You probably want to build for both targets webserver and shell
#
# As of 2026-09-18, stage sizes are:
#
# base:               467MB
#   build:           4.06GB
#   shell:           2.04GB
#   webserver-build: 4.09GB
#   webserver:       2.07GB
#
# Rob, use:
#   docker build --target webserver -t registry.nersc.gov/m1727/raknop/fastdb-webap:...
#   docker build --target shell -t registry.nersc.gov/m1727/raknop/fastdb-shell:...
#
# Some notes
#
# * The shell binary does NOT include mongosh, because that's a
#   gratuitous few hundred MB.  (It's got some Node.js stuff underneath
#   built in; ugh!)  Just use pymongo for interaction with mongodb.  (I
#   mean, the apt postgresql-client package only adds 10MB to the docker
#   image.  Mongo is a monster.)

# ======================================================================
FROM debian:trixie-20260824 AS base
LABEL maintainer="Rob Knop <rknop@pobox.com>"

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

RUN  apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install -y \
         bzip2 \
         ca-certificates \
         curl \
         gnupg \
         less \
         make \
         nano \
         netcat-openbsd \
         postgresql-client \
         procps \
         python3 \
         rlwrap \
         socat \
         tmux \
    && apt-get -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN ln -s /usr/bin/python3 /usr/bin/python
ENV LESS=-XLRi


# # ======================================================================
# FROM base AS mongosh
#
# # Install mongosh (which adds nearly 400MB to the docker image :( )
#
# RUN curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
#    gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
#    --dearmor
#
# RUN echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] http://repo.mongodb.org/apt/debian trixie/mongodb-org/8.0 main" | tee /etc/apt/sources.list.d/mongodb-org-8.0.list
#
# RUN apt-get update \
#   && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
#           mongodb-mongosh \
#  && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*
#
# # I would have installed mongodb-database-tools here, but it isn't
# #   always in the archive I just added!  It's missing for trixie, and
# #   even for bookworm it's missing for arm64.  This mongo archive is
# #   kind of a disaster.  Installation of mongodump and mongorestore has
# #   been moved to build.

# ======================================================================
FROM base AS build

WORKDIR /usr/src

RUN DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get update \
    && DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get -y install -y python3-pip python3-venv git libpq-dev golang

RUN mkdir /venv
RUN python3 -mvenv /venv

# TODO -- fink-client 11 is out, try it
# Also look for and try other package updates
RUN source /venv/bin/activate && \
    pip --no-cache install \
       "apscheduler>=3.11.3,<4.0.0" \
       "astropy>=7.2.2,<8.0.0" \
       "confluent_kafka==2.15.0,<3.0.0" \
       "fastavro>=1.10.0,<2.0.0" \
       "fink-client>=10.1,<11.0" \
       "healpy>=1.20.0,<2.0.0" \
       "nested-pandas>=0.7.1,<0.8.0" \
       "numpy>=2.5.1,<3.0.0" \
       "pandas>=3.0.5,<4.0.0" \
       "psycopg>=3.3.4,<4.0.0" \
       "pycryptodome>=3.23.0,<4.0.0" \
       "pymongo>=4.17.0,<5.0.0" \
       "pytest==9.1.1,<10.0.0" \
       "pytest-timestamper==0.0.10" \
       "python-dateutil==2.9.0.post0" \
       "pytz==2026.3.post1" \
       "remote-pdb>=2.1.0,<3.0.0" \
       "requests==2.34.2,<3.0.0" \
       "simplejson>=3.20.2,<4.0.0"

#       "polars>=1.43.0,<2.0.0" \

# Our own customization of pubsub in pittgoogle-client.
# Check it out and diff vs. upstream to see what's different.
# We have put in a PR to integrate this with standard
# pittgoogle-client, but it may not be accepted.
# ...maybe should move this from my (rknop's) git account
# to LSSTDESC?  Wait and see if the PR is accepted.
RUN source /venv/bin/activate \
    && git clone https://github.com/rknop/pittgoogle-client.git \
    && cd pittgoogle-client \
    && git checkout fastdb \
    && pip install . \
    && cd .. \
    && rm -rf pittgoogle-client \
    && deactivate

# Install mongodump and mongorestore.  Fortunately these only totaly ~40MB,
#   unlike the monster mongosh.
# (Even though this needs golang installed for ./make build to even run,
#  the first thing it does it download go....)
RUN git clone https://github.com/mongodb/mongo-tools.git \
  && cd mongo-tools \
  && ./make build \
  && cp -p bin/mongodump /usr/local/bin \
  && cp -p bin/mongorestore /usr/local/bin \
  && cd .. \
  && rm -rf mongo-tools

# lsdb in PyPi requires older versions of pandas and nested_pandas than
# we want.  This dev branch has updates in pyproject.toml
RUN source /venv/bin/activate \
   && pip --no-cache install "hats @ git+https://github.com/astronomy-commons/hats.git@pandas3" \
                             "lsdb @ git+https://github.com/astronomy-commons/lsdb.git@pandas3"

# # ======================================================================
# # This is for the test webserver.  It installs crappy keys so you
# #   can connect via https.
# # The production webserver is going to be on Spin and won't do
# #   SSL management itself (the spin ingress handles that).
# # (
# FROM base AS test
#
# COPY --from=build /venv/ /venv/
# COPY --from=build /usr/local/bin/mongodump /usr/local/bin/mongodump
# COPY --from=build /usr/local/bin/mongorestore /usr/local/bin/mongorestore
# ENV PATH=/venv/bin:$PATH
#
# RUN mkdir -p /fastdb/webserver
# ENV PYTHONPATH=/fastdb
# WORKDIR /fastdb/webserver
#
# RUN mkdir /sessions
#
# COPY docker_flask/createdb.py /usr/src/createdb.py
# COPY docker_flask/key.pem /usr/src/key.pem
# COPY docker_flask/cert.pem /usr/src/cert.pem
#
# EXPOSE 8080
# ENTRYPOINT [ "gunicorn", "--certfile", "/usr/src/cert.pem", "--keyfile", "/usr/src/key.pem", \
#            "-w", "1", "--threads", "10", "-b", "0.0.0.0:8080", "--timeout", "0", "server:app" ]

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
FROM build AS webserver-build

RUN source /venv/bin/activate && \
    pip --no-cache install \
       "flask>=3.1.3,<4.0.0" \
       "flask-session>=0.8.0,<0.9.0" \
       "flask-sock>=0.7.0,<0.8.0" \
       "gevent>=26.7.0,<27.0.0" \
       "gunicorn>=25.3.0,<26.0.0"


# ======================================================================
# This is for the webserver
FROM base AS webserver

COPY --from=webserver-build /venv/ /venv/
COPY --from=webserver-build /usr/local/bin/mongodump /usr/local/bin/mongodump
COPY --from=webserver-build /usr/local/bin/mongorestore /usr/local/bin/mongorestore
ENV PATH=/venv/bin:$PATH

RUN mkdir -p /fastdb/webserver
ENV PYTHONPATH=/fastdb
WORKDIR /fastdb/webserver

RUN mkdir /sessions

EXPOSE 8080
# ENTRYPOINT [ "/venv/bin/gunicorn", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "40", "server:app" ]
ENTRYPOINT [ "/venv/bin/gunicorn", "-b", "0.0.0.0:8080", "-k", "gevent", "--timeout", "300", "--workers", "2", "server:app" ]
