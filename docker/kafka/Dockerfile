# This is the Dockerfile for the throwaway kafka server used in tests
# If the you change the FROM image, make sure to look at its dockerfile
# for things like users.

FROM apache/kafka:4.2.0

USER root

# Install netcat for the docker compose health check
RUN apk update \
  && apk add netcat-openbsd

USER appuser
