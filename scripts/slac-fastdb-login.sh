USER=${SLAC_USER:?Error: SLAC_USER must be set}
TOKEN=${SLAC_TOKEN:?Error: SLAC_TOKEN must be set visit https://k8s.slac.stanford.edu/desc-fastdb}
kubectl config set-cluster "desc-fastdb" --server=https://k8s.slac.stanford.edu:443/api/desc-fastdb
kubectl config set-credentials "${USER}@slac.stanford.edu@desc-fastdb"  \
    --auth-provider=oidc  \
    --auth-provider-arg='idp-issuer-url=https://dex.slac.stanford.edu'  \
    --auth-provider-arg='client-id=vcluster--desc-fastdb'  \
    --auth-provider-arg='client-secret=rfiVSsKWmuOoRi0MZTORvnjFFdeAB6' \
    --auth-provider-arg='refresh-token=' \
    --auth-provider-arg='id-token='"${TOKEN}"
kubectl config set-context "desc-fastdb" --cluster="desc-fastdb" --user="${USER}@slac.stanford.edu@desc-fastdb"
kubectl config use-context "desc-fastdb"
