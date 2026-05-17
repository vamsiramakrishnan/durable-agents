# Tenancy

The DX-correct surface for future *hard* multi-tenancy. Today the proto and stores scope rows by
`(app_name, user_id, session_id)`; `hard_multi_tenant` mode is design-only and `tape doctor`
warns loudly about it.

```python
import tape

cfg = tape.TenancyConfig(mode=tape.TenancyMode.HARD_MULTI_TENANT, tenant_id="acme")
for w in cfg.warn_if_hard_but_unenforced():
    print("warn:", w)
```

See [Tenancy guide](../../tenancy.md) for the deployment-level discussion.

::: tape.tenancy
    options:
      heading_level: 2
      members_order: source
      show_root_heading: false
