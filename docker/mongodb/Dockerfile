FROM rknop/devuan-daedalus-rknop AS base
LABEL maintainer="Rob Knop <raknop@lbl.gov>"

SHELL ["/bin/bash", "-c"]

WORKDIR /usr/src

# Base System

RUN apt-get update \
  && DEBIAN_FRONTEND="nointeractive" apt-get -y upgrade \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
         gnupg curl sudo netcat-openbsd tmux locales \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN cat /etc/locale.gen | perl -pe 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' > /etc/locale.gen.new \
    && mv /etc/locale.gen.new /etc/locale.gen
RUN locale-gen en_US.utf8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

ENV LESS=-XLRi

# Install Mongo

RUN curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

RUN echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] http://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.0 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

RUN apt-get update \
  && DEBIAN_FRONTEND="noninteractive" TZ="UTC" apt-get -y install -y --no-install-recommends \
          mongodb-org \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


COPY mongod.conf /etc/mongod.conf
RUN mkdir -p /data/db
RUN chown -R mongodb:mongodb /data
RUN mkdir -p /var/log/mongodb
RUN chown -R mongodb:mongodb /var/log/mongodb

# The startup script will run as mongod and will need to be able to write log files,
#   so make mongod a real user and give it a home directory
RUN mkdir /home/mongodb && chown -R mongodb:mongodb /home/mongodb
RUN usermod -d /home/mongodb -s /bin/bash mongodb
WORKDIR /home/mongodb

COPY run_mongo.sh /home/mongodb/

USER mongodb
ENTRYPOINT ["/bin/bash", "run_mongo.sh"]
# Use this for testing
# ENTRYPOINT ["tail", "-f", "/etc/issue"]
