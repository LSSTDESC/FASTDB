#!/bin/bash
HOST=minio-site1.rearmstr-dev.development.svc.spin.nersc.org
NS=rearmstr-dev

openssl s_client -showcerts -connect ${HOST}:443 -servername ${HOST} </dev/null 2>/dev/null \
  | awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/ {print}' > ingress-ca.crt

# awk '
# /BEGIN CERTIFICATE/ {n++}
# n==2 {print}
# /END CERTIFICATE/ && n==2 {exit}
# ' allcerts.pem > ingress-ca.crt

kubectl create secret generic pgbackrest-ca \
  --from-file=ca.crt=ingress-ca.crt \
  -n ${NS} \
  --dry-run=client -o yaml | kubectl apply -f -