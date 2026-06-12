#!/usr/bin/env bash
# scripts/setup-kind-certs.sh
#
# Generate local self-signed certs for FASTDB Kind deployments and create:
#
#   1. minio-certs
#      Legacy/generic secret containing:
#        ca.crt
#        public.crt
#        private.key
#
#   2. minio-proxy-tls
#      Kubernetes TLS secret for nginx MinIO proxy:
#        tls.crt
#        tls.key
#
#   3. pgbackrest-ca
#      CA bundle mounted into postgres/standby:
#        ca.crt
#
# Usage:
#   ./scripts/setup-kind-certs.sh [NAMESPACE]
#   ./scripts/setup-kind-certs.sh fastdb-local --force
#
# Notes:
#   --force regenerates certs. Use this after changing SAN names.

set -euo pipefail

NAMESPACE="${1:-fastdb-local}"
FORCE="${2:-}"

CERT_DIR="certs"
CA_KEY="${CERT_DIR}/ca.key"
CA_CRT="${CERT_DIR}/ca.crt"
SERVER_KEY="${CERT_DIR}/server.key"
SERVER_CSR="${CERT_DIR}/server.csr"
SERVER_CRT="${CERT_DIR}/server.crt"
PUBLIC_CRT="${CERT_DIR}/public.crt"
PRIVATE_KEY="${CERT_DIR}/private.key"
OPENSSL_CONF="${CERT_DIR}/openssl-san.cnf"

if [[ "${FORCE}" == "--force" ]]; then
  echo "--- Removing existing certs ---"
  rm -rf "${CERT_DIR}"
fi

mkdir -p "${CERT_DIR}"

if [[ ! -f "${CA_CRT}" || ! -f "${SERVER_CRT}" || ! -f "${SERVER_KEY}" ]]; then
  echo "--- Generating FASTDB local CA and server certificate ---"

  cat > "${OPENSSL_CONF}" <<EOF
[ req ]
default_bits       = 2048
prompt             = no
default_md         = sha256
distinguished_name = dn
req_extensions     = req_ext

[ dn ]
CN = minio-site1-proxy

[ req_ext ]
subjectAltName = @alt_names

[ v3_req ]
subjectAltName = @alt_names

[ alt_names ]
# New local Kind proxy names. pgBackRest should use these.
DNS.1  = minio-site1-proxy
DNS.2  = minio-site1-proxy.${NAMESPACE}
DNS.3  = minio-site1-proxy.${NAMESPACE}.svc
DNS.4  = minio-site1-proxy.${NAMESPACE}.svc.cluster.local

DNS.5  = minio-site2-proxy
DNS.6  = minio-site2-proxy.${NAMESPACE}
DNS.7  = minio-site2-proxy.${NAMESPACE}.svc
DNS.8  = minio-site2-proxy.${NAMESPACE}.svc.cluster.local

# Legacy/direct MinIO service names. Keep these for compatibility.
DNS.9  = minio-site1
DNS.10 = minio-site1.${NAMESPACE}
DNS.11 = minio-site1.${NAMESPACE}.svc
DNS.12 = minio-site1.${NAMESPACE}.svc.cluster.local

DNS.13 = minio-site2
DNS.14 = minio-site2.${NAMESPACE}
DNS.15 = minio-site2.${NAMESPACE}.svc
DNS.16 = minio-site2.${NAMESPACE}.svc.cluster.local

DNS.17 = minio
DNS.18 = localhost
IP.1   = 127.0.0.1
EOF

  openssl genrsa -out "${CA_KEY}" 2048

  openssl req -new -x509 \
    -key "${CA_KEY}" \
    -out "${CA_CRT}" \
    -days 3650 \
    -sha256 \
    -subj "/CN=FASTDB-Kind-MinIO-CA"

  openssl genrsa -out "${SERVER_KEY}" 2048

  openssl req -new \
    -key "${SERVER_KEY}" \
    -out "${SERVER_CSR}" \
    -config "${OPENSSL_CONF}"

  openssl x509 -req \
    -in "${SERVER_CSR}" \
    -CA "${CA_CRT}" \
    -CAkey "${CA_KEY}" \
    -CAcreateserial \
    -out "${SERVER_CRT}" \
    -days 3650 \
    -sha256 \
    -extensions v3_req \
    -extfile "${OPENSSL_CONF}"

  cp "${SERVER_CRT}" "${PUBLIC_CRT}"
  cp "${SERVER_KEY}" "${PRIVATE_KEY}"
else
  echo "--- Reusing existing certs in ${CERT_DIR}/ ---"
  echo "Use --force to regenerate."
fi

echo "--- Verifying generated server certificate SANs ---"
openssl x509 -in "${SERVER_CRT}" -noout -subject -issuer -dates
openssl x509 -in "${SERVER_CRT}" -noout -ext subjectAltName

echo "--- Creating namespace ${NAMESPACE} if needed ---"
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "--- Creating/updating minio-certs secret ---"
kubectl create secret generic minio-certs \
  --from-file=ca.crt="${CA_CRT}" \
  --from-file=public.crt="${PUBLIC_CRT}" \
  --from-file=private.key="${PRIVATE_KEY}" \
  -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "--- Creating/updating minio-proxy-tls secret ---"
kubectl create secret tls minio-proxy-tls \
  --cert="${PUBLIC_CRT}" \
  --key="${PRIVATE_KEY}" \
  -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "--- Creating/updating pgbackrest-ca secret ---"
kubectl create secret generic pgbackrest-ca \
  --from-file=ca.crt="${CA_CRT}" \
  -n "${NAMESPACE}" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "--- Labelling/annotating secrets for Helm compatibility ---"
for secret in minio-certs minio-proxy-tls pgbackrest-ca; do
  kubectl label secret "${secret}" -n "${NAMESPACE}" \
    app.kubernetes.io/managed-by=Helm --overwrite

  kubectl annotate secret "${secret}" -n "${NAMESPACE}" \
    meta.helm.sh/release-name=fastdb \
    meta.helm.sh/release-namespace="${NAMESPACE}" \
    --overwrite
done

echo "--- Final secrets ---"
kubectl get secret -n "${NAMESPACE}" minio-certs minio-proxy-tls pgbackrest-ca

echo "Certs ready in namespace ${NAMESPACE}"