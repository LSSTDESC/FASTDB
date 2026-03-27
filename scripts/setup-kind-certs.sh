#!/bin/bash
# scripts/setup-kind-certs.sh
# Generate self-signed certs and create the Kubernetes secret

NAMESPACE=${1:-fastdb-local}

# Generate certs if they don't exist
if [ ! -f certs/ca.crt ]; then
    mkdir -p certs && cd certs
    openssl genrsa -out ca.key 2048
    openssl req -new -x509 -key ca.key -out ca.crt -days 3650 -subj "/CN=MinIO-CA"
    openssl genrsa -out server.key 2048
    openssl req -new -key server.key -out server.csr -subj "/CN=minio"
    cat > san.ext <<EOF
subjectAltName=DNS:minio-site1,DNS:minio-site2,DNS:localhost
EOF
    openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
        -CAcreateserial -out server.crt -days 3650 -sha256 -extfile san.ext
    cp server.crt public.crt
    cp server.key private.key
    cd ..
fi

# Create namespace if needed
kubectl create namespace $NAMESPACE 2>/dev/null

# Create or update the secret
kubectl create secret generic minio-certs \
    --from-file=ca.crt=certs/ca.crt \
    --from-file=public.crt=certs/public.crt \
    --from-file=private.key=certs/private.key \
    -n $NAMESPACE \
    --dry-run=client -o yaml | kubectl apply -f -

# Label it so Helm doesn't complain
kubectl label secret minio-certs -n $NAMESPACE \
    app.kubernetes.io/managed-by=Helm --overwrite
kubectl annotate secret minio-certs -n $NAMESPACE \
    meta.helm.sh/release-name=fastdb \
    meta.helm.sh/release-namespace=$NAMESPACE --overwrite

echo "Certs ready in namespace $NAMESPACE"