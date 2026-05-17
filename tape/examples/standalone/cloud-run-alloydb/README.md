# cloud-run-alloydb

A `tape.yaml`-shaped deployment shape that points at the
`cloud-run-alloydb` Terraform example. Use as a starting point for production
deployments backed by AlloyDB.

```bash
tape provision gcp --store alloydb --target cloud-run --apply
tape deploy gcp --target cloud-run
tape doctor --gcp
```
