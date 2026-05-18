package chaos

import (
	"fmt"
	"strings"
)

// ReliabilitySurface — R(k, ε, λ). Mirrors `tape.chaos.reliability`.
//
//	k       scenarios driven
//	Epsilon invariant-violation rate
//	Lambda  recovery rate
type ReliabilitySurface struct {
	K       int
	Epsilon float64
	Lambda  float64
}

func (s ReliabilitySurface) String() string {
	return fmt.Sprintf("R(k=%d, ε=%.2f, λ=%.2f)", s.K, s.Epsilon, s.Lambda)
}

type recorderRow struct {
	ScenarioName     string
	Passed           bool
	FailedInvariants []string
	Terminal         bool
	Notes            []string
}

// Recorder — accumulator for chaos campaign results.
type Recorder struct {
	rows []recorderRow
}

// NewRecorder — fresh recorder.
func NewRecorder() *Recorder { return &Recorder{} }

// AddOpts — knobs for Recorder.Add.
type AddOpts struct {
	Terminal bool // true = the run reached a terminal state despite faults
}

// Add — record one scenario's outcome.
func (r *Recorder) Add(report ChaosReport, opts AddOpts) {
	failed := make([]string, 0)
	for _, ir := range report.InvariantResults {
		if !ir.Passed {
			failed = append(failed, ir.Name)
		}
	}
	r.rows = append(r.rows, recorderRow{
		ScenarioName:     report.ScenarioName,
		Passed:           report.Passed,
		FailedInvariants: failed,
		Terminal:         opts.Terminal,
		Notes:            append([]string(nil), report.Notes...),
	})
}

// Surface — the current R(k, ε, λ).
func (r *Recorder) Surface() ReliabilitySurface {
	k := len(r.rows)
	if k == 0 {
		return ReliabilitySurface{K: 0, Epsilon: 0, Lambda: 1}
	}
	violations := 0
	terminal := 0
	for _, row := range r.rows {
		if !row.Passed {
			violations++
		}
		if row.Terminal {
			terminal++
		}
	}
	return ReliabilitySurface{
		K:       k,
		Epsilon: float64(violations) / float64(k),
		Lambda:  float64(terminal) / float64(k),
	}
}

// ToMarkdownOpts — knobs for ToMarkdown.
type ToMarkdownOpts struct {
	Title string // default: "TapeChaos campaign"
}

// ToMarkdown — render the campaign as a Markdown table.
func (r *Recorder) ToMarkdown(opts ToMarkdownOpts) string {
	title := opts.Title
	if title == "" {
		title = "TapeChaos campaign"
	}
	s := r.Surface()
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", title)
	fmt.Fprintf(&b, "**Reliability Surface**: `R(k=%d, ε=%.2f, λ=%.2f)`\n\n", s.K, s.Epsilon, s.Lambda)
	fmt.Fprintf(&b, "- %d scenarios\n", s.K)
	fmt.Fprintf(&b, "- %d invariant violations\n", int(s.Epsilon*float64(s.K)+0.5))
	fmt.Fprintf(&b, "- %d runs reached terminal\n\n", int(s.Lambda*float64(s.K)+0.5))
	fmt.Fprintln(&b, "| Scenario | Passed | Terminal | Failed invariants |")
	fmt.Fprintln(&b, "|---|---|---|---|")
	for _, row := range r.rows {
		passed := "FAIL"
		if row.Passed {
			passed = "OK"
		}
		terminal := "no"
		if row.Terminal {
			terminal = "yes"
		}
		failed := strings.Join(row.FailedInvariants, ", ")
		if failed == "" {
			failed = "—"
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s |\n",
			row.ScenarioName, passed, terminal, failed)
	}
	return b.String()
}

// Score — quick R(k, ε, λ) for a list of reports.
func Score(reports []ChaosReport) ReliabilitySurface {
	rec := NewRecorder()
	for _, r := range reports {
		rec.Add(r, AddOpts{Terminal: true})
	}
	return rec.Surface()
}
