# Deploying {{ name }} to GCP

```bash
# 1. infrastructure (idempotent; safe to re-run)
tape provision gcp --dry-run        # render Terraform — review the plan
tape provision gcp --apply          # apply

# 2. images + Cloud Run services
tape deploy gcp --target cloud-run  # render Cloud Run service specs

# 3. verify
tape doctor --gcp                   # APIs, SAs, secrets, server reachability
tape status                         # runs, effects, obligations, reactor lag
```

Generated artifacts live under `deploy/gcp/terraform/` (Terraform) and
`deploy/gcp/release/` (Cloud Run service specs). Commit them.
