# gke-bigtable

A `tape.yaml`-shaped deployment shape that targets GKE Autopilot + Bigtable.
Uses the bundled Helm chart.

```bash
tape provision gcp --target gke --store bigtable --apply
tape deploy gcp --target gke
helm upgrade --install tape tape/deploy/gcp/k8s/chart/tape \
  -n tape --create-namespace \
  -f deploy/gcp/release/values.generated.yaml
```
