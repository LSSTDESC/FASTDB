#
# DOCKER_BUILDKIT=1 docker build --target <target> -t <image>:<label>.
#
# You probably want to build for both targets webserver and shell
#
# Rob, use:
#   DOCKER_BUILDKIT=1 docker build --target webserver -t registry.nersc.gov/m1727/raknop/fastdb-webap:rknop-dev .
#   DOCKER_BUILDKIT=1 docker build --target shell -t registry.nersc.gov/m1727/raknop/fastdb-shell:rknop-dev .

FROM rknop/devuan-daedalus-rknop AS base
LABEL maintainer="Rob Knop <rknop@pobox.com>"

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

RUN  apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install -y \
         python3 locales tmux netcat-openbsd gnupg curl elinks postgresql-client make rlwrap socat \
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


# Install mongosh (which adds >250MB to the docker image :( )

RUN curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

RUN echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] http://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.0 main" | tee /etc/apt/sources.list.d/mongodb-org-8.0.list

RUN apt-get update \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
          mongodb-mongosh \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


# ======================================================================
FROM base AS build

RUN DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get update \
    && DEBIAN_FRONTEND="noninteractive" TZ="UTC" \
    apt-get -y install -y python3-pip python3-venv git libpq-dev

RUN mkdir /venv
RUN python3 -mvenv /venv

RUN source /venv/bin/activate && \
    pip --no-cache install \
       apscheduler \
       astropy \
       flask \
       flask-session \
       flask-sock \
       gevent \
       gunicorn \
       numpy \
       pandas \
       psycopg2 \
       pycryptodome \
       pymongo \
       pytest \
       pytest-timestamper \
       python-dateutil \
       pytz \
       remote-pdb \
       requests \
       simplejson

# ======================================================================
# This is for the test webserver.  It installs crappy keys so you
#   can connect via https.
# The production webserver is going to be on Spin and won't do
#   SSL management itself (the spin ingress handles that).
FROM base AS test

COPY --from=build /venv/ /venv/
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
ENV PATH=/venv/bin:$PATH

RUN mkdir -p /fastdb/webserver
ENV PYTHONPATH=/fastdb
WORKDIR /fastdb

ENTRYPOINT [ "tail", "-f", "/etc/issue" ]

# ======================================================================
# This is for the production webserver
FROM base AS webserver

COPY --from=build /venv/ /venv/
ENV PATH=/venv/bin:$PATH

RUN mkdir -p /fastdb/webserver
ENV PYTHONPATH=/fastdb
WORKDIR /fastdb/webserver

RUN mkdir /sessions

EXPOSE 8080
ENTRYPOINT [ "/venv/bin/gunicorn", "-b", "0.0.0.0:8080", "--access-logfile", "-", "--error-logfile", "-", "-k", "gevent", "--timeout", "300", "--workers", "40", "server:app" ]
