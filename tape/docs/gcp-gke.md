# Tape on GKE Autopilot

```bash
tape provision gcp --target gke --store bigtable --apply
tape deploy gcp --target gke --image-tag 0.2
# wrote: deploy/gcp/release/values.generated.yaml

helm upgrade --install tape tape/deploy/gcp/k8s/chart/tape \
  --namespace tape --create-namespace \
  -f deploy/gcp/release/values.generated.yaml
```

The chart layout:

```
tape/deploy/gcp/k8s/chart/tape/
  Chart.yaml
  values.yaml
  templates/
    _helpers.tpl
    serviceaccounts.yaml      # WI-bound to tape-server/-reactor GSAs
    server.yaml               # Deployment + Service + HPA + PDB
    reactors.yaml             # one Deployment per enabled reactor
    networkpolicy.yaml
```

## Workload Identity

Set `workloadIdentity.serverGsa` and `workloadIdentity.reactorGsa` to the GSA
emails created by the IAM Terraform module. The chart wires the KSAs with the
`iam.gke.io/gcp-service-account` annotation; you still need to grant
`roles/iam.workloadIdentityUser` on each GSA to the KSA member
(`serviceAccount:PROJECT.svc.id.goog[tape/tape-server]` and `.../tape-reactor`).

## Kustomize overlays

For the self-managed manifest in `tape/deploy/k8s/tape.yaml`, store-specific
overlays live in `tape/deploy/gcp/k8s/overlays/{alloydb,bigtable,spanner}/`.
They're useful if you'd rather not pull in Helm; Helm is the recommended path.
