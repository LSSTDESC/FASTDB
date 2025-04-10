#
# DOCKER_BUILDKIT=1 docker build --target queryrunner -t <image>:<label>.
#
# Rob, use:
# DOCKER_BUILDKIT=1 docker build --target queryrunner -t registry.nersc.gov/m1727/raknop/fastdb-query-runner:rknop-dev .

FROM rknop/devuan-daedalus-rknop AS base
LABEL maintainer="Rob Knop <rknop@pobox.com>"

SHELL ["/bin/bash", "-c"]

ENV DEBIAN_FRONTEND="noninteractive"
ENV TZ="UTC"

RUN  apt-get update \
    && apt-get -y upgrade \
    && apt-get -y install -y \
         python3 tmux postgresql-client \
    && apt-get -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python
ENV LESS=-XLRi

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
       numpy \
       pandas \
       psycopg==3.2.6

# ======================================================================
FROM base AS queryrunner

COPY --from=build /venv/ /venv/
ENV PATH=/venv/bin:$PATH

RUN mkdir /fastdb
ENV PYTHONPATH=/fastdb
WORKDIR /fastdb/services

ENTRYPOINT [ "python", "long_query_runner.py", "--loop", "--sleep-time", "10", "--num-runners", "10" ]
