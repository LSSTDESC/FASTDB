#
# DOCKER_BUILDKIT=1 docker build --target postgres -t <image>:<label> .
#
# Rob, use:
#   DOCKER_BUILDKIT=1 docker build --target postgres -t registry.nersc.gov/m1727/raknop/fastdb-postgres:rknop-dev .
#

FROM debian:bookworm-20250630 AS base
LABEL maintainer="Rob Knop <raknop@lbl.gov>"

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
  && DEBIAN_FRONTEND="nointeractive" apt-get -y upgrade \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
      postgresql-15 postgresql-client-15 pgtop libssl3 libreadline8 zlib1g tmux netcat-openbsd \
      libzstd1 liblz4-1 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/lib/postgresql/data \
  && chmod 700 /var/lib/postgresql/data \
  && chown postgres:postgres /var/lib/postgresql/data

RUN cat /etc/locale.gen | perl -pe 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' > /etc/locale.gen.new \
    && mv /etc/locale.gen.new /etc/locale.gen
RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8


RUN echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/15/main/pg_hba.conf
COPY postgresql.conf /etc/postgresql/15/main/postgresql.conf
ENV POSTGRES_DATA_DIR=/var/lib/postgresql/data

ENV LESS=-XLRi

# ======================================================================
FROM base AS build

RUN mkdir /build
WORKDIR /build

RUN apt-get update \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
      make git gcc libssl-dev libreadline-dev zlib1g-dev libzstd-dev liblz4-dev curl \
      postgresql-server-dev-15 ca-certificates

RUN git clone https://github.com/segasai/q3c.git
RUN cd q3c \
  && make \
  && make install

RUN curl -L https://github.com/ossc-db/pg_hint_plan/archive/refs/tags/REL15_1_5_1.tar.gz \
         -o pg_hint_plan-REL15_1_5_1.tar.gz \
  && tar -xpf pg_hint_plan-REL15_1_5_1.tar.gz \
  && cd pg_hint_plan-REL15_1_5_1 \
  && make \
  && make install

RUN git clone https://github.com/segasai/pg_healpix.git /build/pg_healpix \
 && cd /build/pg_healpix \
 && make USE_PGXS=1 \
 && make USE_PGXS=1 install \
 && rm -rf /build/pg_healpix

# ======================================================================
FROM base AS postgres

COPY --from=build /usr/lib/postgresql/15/lib/q3c.so /usr/lib/postgresql/15/lib/q3c.so
COPY --from=build /usr/lib/postgresql/15/lib/bitcode/q3c /usr/share/postgresql/lib/bitcode/q3c
COPY --from=build /usr/lib/postgresql/15/lib/bitcode/q3c.index.bc /usr/share/postgresql/lib/bitcode/q3c.index.bc
COPY --from=build /usr/share/postgresql/15/extension/q3c* /usr/share/postgresql/15/extension/
COPY --from=build /usr/lib/postgresql/15/lib/pg_hint_plan.so usr/lib/postgresql/15/lib/pg_hint_plan.so
COPY --from=build /usr/lib/postgresql/15/lib/bitcode/pg_hint_plan /usr/share/postgresql/lib/bitcode/pg_hint_plan
COPY --from=build /usr/lib/postgresql/15/lib/bitcode/pg_hint_plan.index.bc /usr/share/postgresql/lib/bitcode/pg_hint_plan.index.bc
COPY --from=build /usr/share/postgresql/15/extension/pg_hint_plan* /usr/share/postgresql/15/extension/
# pg_healpix extension
COPY --from=build /usr/lib/postgresql/15/lib/pg_healpix.so /usr/lib/postgresql/15/lib/pg_healpix.so
COPY --from=build /usr/share/postgresql/15/extension/pg_healpix* /usr/share/postgresql/15/extension/

# Make sure this matches what is in the config file (created just above)
# (There is some futzing about here to make sure the right permissions are
# on the file regardless of the permissions as it exists on the machine
# where the docker image is built.  I don't know why a simple ADD
# followed by CHMOD didn't work.)
COPY run_postgres.sh /tmp/run_postgres.sh
RUN cp /tmp/run_postgres.sh /run_postgres.sh && chmod 755 /run_postgres.sh

USER postgres
CMD ["bash", "/run_postgres.sh"]

# ======================================================================

FROM base AS pgdump

RUN mkdir=/home/pgdump
ENV HOME=/home/pgdump
ADD run_pgdump.sh /home/pgdump/run_pgdump.sh

USER postgres
ENTRYPOINT [ "/bin/bash", "/home/pgdump/run_pgdump.sh" ]
