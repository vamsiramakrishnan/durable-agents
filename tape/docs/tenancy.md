# Tenancy

```yaml
tenancy:
  mode: single        # single | trusted_multi_app | hard_multi_tenant
  tenant_id: default
```

## `single`

One tenant, one set of apps, one cluster. The default. Authz is at the IAM
level (who can call the Tape server).

## `trusted_multi_app`

Multiple apps from the same trust domain co-tenant on one Tape cluster.
Rows are scoped by `(app_name, user_id, session_id)`. Cross-app reads are not
prevented by the runtime; trust comes from the deploy boundary.

## `hard_multi_tenant`

You want one Tape cluster to serve mutually-untrusting tenants. This requires
**tenant_id in the proto, the stores, and the authz layer**. The proto change
is on the roadmap. `tape doctor` will warn loudly if you select this mode
today — the warning text is intentionally blunt:

> tenancy.mode=hard_multi_tenant requested but the Tape proto and stores do
> not yet carry a first-class tenant_id. Cross-tenant data isolation cannot
> be enforced at the runtime; this mode is DESIGN-ONLY today.

Use `single` or `trusted_multi_app` until the proto change ships.
