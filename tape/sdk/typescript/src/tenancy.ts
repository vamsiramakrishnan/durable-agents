// Tenancy — the DX-correct surface for *future* hard multi-tenancy.
// Mirrors `tape.tenancy` in Python.

export type TenancyMode = 'single' | 'trusted_multi_app' | 'hard_multi_tenant';

export interface TenancyConfig {
  mode: TenancyMode;
  tenantId: string;
}

export function tenancyDefaults(): TenancyConfig {
  return { mode: 'single', tenantId: 'default' };
}

export function tenancyFromEnv(env: NodeJS.ProcessEnv = process.env): TenancyConfig {
  const mode = (env.TAPE_TENANCY as TenancyMode) || 'single';
  const tenantId = env.TAPE_TENANT_ID || 'default';
  return { mode, tenantId };
}

export function tenancyFromObject(raw: Partial<TenancyConfig> | undefined): TenancyConfig {
  if (!raw) return tenancyDefaults();
  return { mode: raw.mode ?? 'single', tenantId: raw.tenantId ?? 'default' };
}

export function isHard(t: TenancyConfig): boolean { return t.mode === 'hard_multi_tenant'; }

/**
 * Return loud warnings when `hard_multi_tenant` is requested but the
 * runtime can't enforce isolation (the proto / stores don't carry
 * tenant_id yet). Empty list means OK.
 */
export function warnIfHardButUnenforced(t: TenancyConfig): string[] {
  if (!isHard(t)) return [];
  return [
    'tenancy.mode=hard_multi_tenant requested but the Tape proto and stores ' +
    'do not yet carry a first-class tenant_id. Cross-tenant data isolation ' +
    'cannot be enforced at the runtime; this mode is DESIGN-ONLY today.',
  ];
}
