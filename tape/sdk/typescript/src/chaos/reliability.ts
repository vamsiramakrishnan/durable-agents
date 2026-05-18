// Reliability Surface — R(k, ε, λ). Mirrors `tape.chaos.reliability`.
//
//   k  scenarios driven
//   ε  invariant-violation rate (0.0 best, 1.0 worst)
//   λ  recovery rate (1.0 best, 0.0 worst)

import type { ChaosReport } from './scenarios.ts';

export interface ReliabilitySurface {
  k: number;
  epsilon: number;
  lambda: number;
  toString(): string;
}

interface Row {
  scenarioName: string;
  passed: boolean;
  failedInvariants: string[];
  terminal: boolean;
  notes: string[];
}

export class Recorder {
  private rows: Row[] = [];

  add(report: ChaosReport, opts: { terminal?: boolean } = {}): void {
    const terminal = opts.terminal ?? true;
    const failed = (report.invariantResults ?? []).filter(ir => !ir.passed).map(ir => ir.name);
    this.rows.push({
      scenarioName: report.scenarioName,
      passed: report.passed,
      failedInvariants: failed,
      terminal,
      notes: [...(report.notes ?? [])],
    });
  }

  get surface(): ReliabilitySurface {
    const k = this.rows.length;
    if (k === 0) return makeSurface(0, 0, 1);
    const violations = this.rows.filter(r => !r.passed).length;
    const terminal = this.rows.filter(r => r.terminal).length;
    return makeSurface(k, violations / k, terminal / k);
  }

  get allRows(): readonly Row[] { return this.rows; }

  /** Render the campaign as a Markdown report — paste-into-PR friendly. */
  toMarkdown(opts: { title?: string } = {}): string {
    const s = this.surface;
    const lines: string[] = [
      `# ${opts.title ?? 'TapeChaos campaign'}`,
      '',
      `**Reliability Surface**: \`R(k=${s.k}, ε=${s.epsilon.toFixed(2)}, λ=${s.lambda.toFixed(2)})\``,
      '',
      `- ${s.k} scenarios`,
      `- ${Math.round(s.epsilon * s.k)} invariant violations`,
      `- ${Math.round(s.lambda * s.k)} runs reached terminal`,
      '',
      '| Scenario | Passed | Terminal | Failed invariants |',
      '|---|---|---|---|',
    ];
    for (const r of this.rows) {
      lines.push(
        `| \`${r.scenarioName}\` | ${r.passed ? 'OK' : 'FAIL'} | ${r.terminal ? 'yes' : 'no'} | ${r.failedInvariants.join(', ') || '—'} |`,
      );
    }
    return lines.join('\n');
  }
}

function makeSurface(k: number, epsilon: number, lambda: number): ReliabilitySurface {
  return {
    k, epsilon, lambda,
    toString() {
      return `R(k=${this.k}, ε=${this.epsilon.toFixed(2)}, λ=${this.lambda.toFixed(2)})`;
    },
  };
}

/** Quick score for a list of reports — same as `new Recorder()` + adds. */
export function score(reports: readonly ChaosReport[]): ReliabilitySurface {
  const rec = new Recorder();
  for (const r of reports) rec.add(r);
  return rec.surface;
}
