# This is the Dockerfile for the throwaway kafka server used in tests

FROM rknop/devuan-daedalus-rknop

LABEL maintainenr="Rob Knop <raknop@lbl.gov>"

SHELL ["/bin/bash", "-c"]
RUN apt-get -y update \
   && apt-get -y upgrade \
   && apt-get -y install openjdk-17-jre curl netcat-openbsd \
   && apt-get clean \
   && rm -rf /var/lib/apt/lists/*

RUN mkdir /kafka
WORKDIR /kafka

ARG KAFKA_CODEVERSION=3.8.0
ARG KAFKA_VERSION=2.13
RUN echo "Downloading https://dlcdn.apache.org/kafka/${KAFKA_CODEVERSION}/kafka_${KAFKA_VERSION}-${KAFKA_CODEVERSION}.tgz"
RUN curl -L https://dlcdn.apache.org/kafka/${KAFKA_CODEVERSION}/kafka_${KAFKA_VERSION}-${KAFKA_CODEVERSION}.tgz -O \
  && tar xvf kafka_${KAFKA_VERSION}-${KAFKA_CODEVERSION}.tgz \
  && rm kafka_${KAFKA_VERSION}-${KAFKA_CODEVERSION}.tgz

COPY server.properties kafka_${KAFKA_VERSION}-${KAFKA_CODEVERSION}/config/
COPY zookeeper.properties kafka_${KAFKA_VERSION}-${KAFKA_CODEVERSION}/config/

WORKDIR /kafka/kafka_${KAFKA_VERSION}-${KAFKA_CODEVERSION}

# No entrypoint, as we're going to use two different entrypoints.  (See ../../docker-compose.yaml)
