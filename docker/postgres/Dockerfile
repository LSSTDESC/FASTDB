#
# NERSCSPIN=1 docker build --target postgres -t <image>:<label> .
#
# Rob, use:
#   NERSCSPIN=1 docker build --target postgres -t registry.nersc.gov/m1727/raknop/fastdb-postgres:rknop-dev .
#
# BUT WARNING, the current (as of 2026-03-16) production FASTDB uses an
#   older version with postgres 15, so be ready to do the database
#   version migration if you're going to deploy this.

FROM debian:trixie-20260223 AS base
LABEL maintainer="Rob Knop <raknop@lbl.gov>"

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
  && DEBIAN_FRONTEND="nointeractive" apt-get -y upgrade \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
      postgresql-17 postgresql-client-17 pgtop libssl3 libreadline8 zlib1g netcat-openbsd \
      libzstd1 liblz4-1 \
      tmux emacs-nox less procps \
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

ENV LESS=-XLRi

# ======================================================================
FROM base AS build

RUN mkdir /build
WORKDIR /build

RUN apt-get update \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
      make flex git gcc libssl-dev libreadline-dev zlib1g-dev libzstd-dev liblz4-dev curl \
      postgresql-server-dev-17 ca-certificates pkg-config

RUN git clone https://github.com/segasai/q3c.git
RUN cd q3c \
  && make \
  && make install

RUN curl -L https://github.com/ossc-db/pg_hint_plan/archive/refs/tags/REL17_1_7_1.tar.gz \
         -o pg_hint_plan-REL17_1_7_1.tar.gz \
  && tar -xpf pg_hint_plan-REL17_1_7_1.tar.gz \
  && cd pg_hint_plan-REL17_1_7_1 \
  && make \
  && make install


ENV CARGO_HOME=/opt/rust
ENV RUSTUP_HOME=/opt/rust
ENV PATH="${PATH}:/opt/rust/bin"
RUN mkdir -p /opt/rust \
  && chown postgres -R /opt/rust

RUN chown postgres -R /usr/share/postgresql/ \
  && chown postgres -R /usr/lib/postgresql

ENV PG_PARQUET_VERSION=0.4.3
ENV PGRX_VERSION=0.16.0
RUN curl https://sh.rustup.rs -sSf | sh -s -- --no-modify-path --profile minimal --default-toolchain stable -y
RUN cargo install --locked cargo-pgrx --version ${PGRX_VERSION}
RUN cargo pgrx init --pg17 $(which pg_config)

RUN cd /opt/rust \
  && curl -LOJ https://github.com/CrunchyData/pg_parquet/archive/refs/tags/v${PG_PARQUET_VERSION}.tar.gz \
  && tar -xvzf pg_parquet-${PG_PARQUET_VERSION}.tar.gz \
  && cd pg_parquet-${PG_PARQUET_VERSION} \
  && cargo pgrx install --release

# ======================================================================
FROM base AS conf

COPY --from=build /usr/lib/postgresql/17/lib/q3c.so /usr/lib/postgresql/17/lib/q3c.so
COPY --from=build /usr/lib/postgresql/17/lib/bitcode/q3c /usr/share/postgresql/lib/bitcode/q3c
COPY --from=build /usr/lib/postgresql/17/lib/bitcode/q3c.index.bc /usr/share/postgresql/lib/bitcode/q3c.index.bc
COPY --from=build /usr/share/postgresql/17/extension/q3c* /usr/share/postgresql/17/extension/
COPY --from=build /usr/lib/postgresql/17/lib/pg_hint_plan.so usr/lib/postgresql/17/lib/pg_hint_plan.so
COPY --from=build /usr/lib/postgresql/17/lib/bitcode/pg_hint_plan /usr/share/postgresql/lib/bitcode/pg_hint_plan
COPY --from=build /usr/lib/postgresql/17/lib/bitcode/pg_hint_plan.index.bc /usr/share/postgresql/lib/bitcode/pg_hint_plan.index.bc
COPY --from=build /usr/share/postgresql/17/extension/pg_hint_plan* /usr/share/postgresql/17/extension/
COPY --from=build /usr/lib/postgresql/17/lib/pg_parquet.so /usr/lib/postgresql/17/lib/
COPY --from=build /usr/share/postgresql/17/extension/pg_parquet* /usr/share/postgresql/17/extension/

RUN echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/17/main/pg_hba.conf

# Have to configure postgres for different amounts of memory for NERSC Spin
# and github CI
ARG NERSCSPIN=0
RUN cp -p /etc/postgresql/17/main/postgresql.conf /etc/postgresql/17/main/postgresql.conf.orig
COPY postgresql.conf /etc/postgresql/17/main/postgresql.conf
RUN cd /etc/postgresql/17/main \
 && if [ "$NERSCSPIN" = "1" ] ; then \
      cat postgresql.conf | sed 's/${config_shared_buffers}/32GB/; s/${config_temp_buffers}/4GB/; s/${config_work_mem}/4GB/; s/${config_maintenance_work_mem}/4GB/;' > postgresql.conf.new; \
    else \
      cat postgresql.conf | sed 's/${config_shared_buffers}/2GB/; s/${config_temp_buffers}/1GB/; s/${config_work_mem}/1GB/; s/${config_maintenance_work_mem}/1GB/;' > postgresql.conf.new ; \
    fi \
 && mv postgresql.conf.new postgresql.conf
 

ENV POSTGRES_DATA_DIR=/var/lib/postgresql/data

# ======================================================================
FROM conf AS postgres

# Make sure the contents of run_postgres.sh what is in postgresql.conf.
# (There is some futzing about here to make sure the right permissions are
# on the file regardless of the permissions as it exists on the machine
# where the docker image is built.  I don't know why a simple ADD
# followed by CHMOD didn't work.)
COPY run_postgres.sh /tmp/run_postgres.sh
RUN cp /tmp/run_postgres.sh /run_postgres.sh && chmod 755 /run_postgres.sh

# Make a temporary working directory that will
#   be made the defaulte temporary tablespace.
RUN mkdir -p /tmp/postgres_temp && chown -R postgres:postgres /tmp/postgres_temp

USER postgres
CMD ["bash", "/run_postgres.sh"]

# ======================================================================

FROM conf AS pgdump

RUN mkdir=/home/pgdump
ENV HOME=/home/pgdump
ADD run_pgdump.sh /home/pgdump/run_pgdump.sh

USER postgres
ENTRYPOINT [ "/bin/bash", "/home/pgdump/run_pgdump.sh" ]
