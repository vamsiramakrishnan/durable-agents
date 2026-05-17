package tape

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/tapepb"
)

// waitForFn polls until the predicate returns true or the deadline expires.
// Returns the final value of the predicate.
func waitForFn(t *testing.T, timeout time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// beginRun + recordDecision give us a journal-producing op that doesn't suffer
// from the (run_id="", seq=0) PK conflict the value-write path has under
// repeated writes (server-side bug; the value journal entry uses run_id="" and
// seq=0 unconditionally). We use a fresh run per test for isolation.
func beginTestRun(ctx context.Context, t *testing.T, c *Client, session string) string {
	t.Helper()
	r, err := c.BeginRun(ctx, BeginRunOpts{
		AppName: "react-test", UserID: "u", SessionID: session,
		InvocationID: fmt.Sprintf("inv-%d", time.Now().UnixNano()),
		LeaseOwner:   "test", LeaseTTLMs: 60_000,
	})
	if err != nil {
		t.Fatalf("BeginRun: %v", err)
	}
	return r.GetRunId()
}

func recordDecision(ctx context.Context, t *testing.T, c *Client, runID string, idx int64) {
	t.Helper()
	if _, err := c.RecordDecision(ctx, runID, idx, "m", "{}", `{"plan":1}`, "", "p1"); err != nil {
		t.Fatalf("RecordDecision(%d): %v", idx, err)
	}
}

// TestBootstrapFromHeadSkipsBacklog: a reaction registered with
// BootstrapFromHead=true skips the backlog and only fires on entries written
// AFTER its registration. We:
//
//  1. Begin a run and record decision 0 — this is the "backlog" entry.
//  2. Register a reaction over `/tape/decision/recorded/<run>/**` with
//     BootstrapFromHead=true.
//  3. Wait briefly — no tasks should appear for decision 0.
//  4. Record decision 1 — this must produce a task.
func TestBootstrapFromHeadSkipsBacklog(t *testing.T) {
	url, stop := startServer(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	runID := beginTestRun(ctx, t, c, "boot-skip")
	// (1) Backlog entry.
	recordDecision(ctx, t, c, runID, 0)
	// Give the journal a moment so the matcher's view of `head` is stable.
	time.Sleep(200 * time.Millisecond)

	// (2) Register with bootstrap_from_head=true.
	r, err := c.RegisterReaction(ctx, RegisterReactionOpts{
		Name:              "boot-skip-reaction",
		SubjectPattern:    fmt.Sprintf("/tape/decision/recorded/%s/**", runID),
		HandlerKind:       HandlerKindTask,
		BootstrapFromHead: true,
	})
	if err != nil {
		t.Fatalf("RegisterReaction: %v", err)
	}
	rid := r.GetReactionId()

	// (3) Verify the backlog was skipped — no tasks should ever appear for
	// decision 0. We sleep enough to give the matcher at least one poll cycle.
	time.Sleep(1500 * time.Millisecond)
	got, err := c.ListTasks(ctx, rid, pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 50)
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(got) != 0 {
		subs := make([]string, 0, len(got))
		for _, x := range got {
			subs = append(subs, x.GetSubject())
		}
		t.Fatalf("bootstrap_from_head=true must skip the backlog; got %d tasks: %v", len(got), subs)
	}

	// (4) New entry — must produce one task.
	recordDecision(ctx, t, c, runID, 1)
	if !waitForFn(t, 5*time.Second, func() bool {
		ts, err := c.ListTasks(ctx, rid, pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 50)
		return err == nil && len(ts) > 0
	}) {
		t.Fatalf("expected a task for the post-registration decision")
	}

	// Sanity: ListReactions includes our reaction.
	all, err := c.ListReactions(ctx, "")
	if err != nil {
		t.Fatalf("ListReactions: %v", err)
	}
	var seen bool
	for _, x := range all {
		if x.GetReactionId() == rid {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("ListReactions did not include the just-registered reaction (rid=%s)", rid)
	}
}

// TestTaskHandlerRunsViaRunDispatcher registers a TASK reaction via the Go
// decorator surface, triggers it via RecordDecision, and verifies the
// dispatcher invokes the user handler and completes the task.
func TestTaskHandlerRunsViaRunDispatcher(t *testing.T) {
	url, stop := startServer(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Tests share a process — isolate the registry.
	ClearRegistry()
	t.Cleanup(ClearRegistry)

	runID := beginTestRun(ctx, t, c, "disp-handler")

	var (
		mu        sync.Mutex
		envelopes []*Envelope
	)
	// `OnDecisionRecorded` subscribes to `/tape/decision/recorded/**`.
	OnDecisionRecorded(
		func(_ context.Context, env *Envelope) error {
			mu.Lock()
			defer mu.Unlock()
			envelopes = append(envelopes, env)
			return nil
		},
		ReactionDef{Name: "disp-handler", MaxConcurrency: 2},
	)
	regs, err := RegisterAll(ctx, c, fmt.Sprintf("t%d-", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	if len(regs) != 1 {
		t.Fatalf("expected 1 registered reaction; got %d", len(regs))
	}
	rid := regs[0].GetReactionId()

	// Trigger.
	recordDecision(ctx, t, c, runID, 0)

	// Wait for the matcher to enqueue.
	if !waitForFn(t, 5*time.Second, func() bool {
		ts, err := c.ListTasks(ctx, rid, pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 50)
		return err == nil && len(ts) > 0
	}) {
		t.Fatalf("matcher never produced a task")
	}

	// One dispatcher pass — claim + run + ack.
	if err := RunDispatcher(ctx, c, RunDispatcherOpts{Once: true}); err != nil {
		t.Fatalf("RunDispatcher: %v", err)
	}

	// Handler may run on a goroutine; wait for it.
	if !waitForFn(t, 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(envelopes) > 0
	}) {
		t.Fatalf("handler was never invoked")
	}

	mu.Lock()
	got := envelopes[0]
	mu.Unlock()
	if !strings.Contains(got.Task.GetSubject(), "/tape/decision/recorded/") {
		t.Fatalf("envelope subject = %q; expected to start with /tape/decision/recorded/",
			got.Task.GetSubject())
	}

	// Task should reach DONE.
	if !waitForFn(t, 5*time.Second, func() bool {
		done, err := c.ListTasks(ctx, rid, pb.TaskStatus_TASK_STATUS_DONE, 50)
		return err == nil && len(done) > 0
	}) {
		ts, _ := c.ListTasks(ctx, rid, pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 50)
		var dbg []string
		for _, x := range ts {
			dbg = append(dbg, fmt.Sprintf("%s(status=%v)", x.GetTaskId(), x.GetStatus()))
		}
		t.Fatalf("task never reached DONE; have %v", dbg)
	}
}

// TestSubscribeBySubjectFiltersByPattern records decisions in two runs, opens
// a SubscribeBySubject stream filtered on one run's subject, and confirms
// only that run's entries land in the stream.
func TestSubscribeBySubjectFiltersByPattern(t *testing.T) {
	url, stop := startServer(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	wantRun := beginTestRun(ctx, t, c, "subj-want")
	otherRun := beginTestRun(ctx, t, c, "subj-other")
	recordDecision(ctx, t, c, wantRun, 0)
	recordDecision(ctx, t, c, otherRun, 0)
	// Give the WAL a moment.
	time.Sleep(200 * time.Millisecond)

	// Subject-filtered tail with a short deadline; the stream drains, then
	// the server holds the conn open until DeadlineExceeded.
	streamCtx, cancelStream := context.WithTimeout(ctx, 2*time.Second)
	defer cancelStream()
	pattern := fmt.Sprintf("/tape/decision/recorded/%s/**", wantRun)
	stream, err := c.SubscribeBySubject(streamCtx, pattern, "", 0)
	if err != nil {
		t.Fatalf("SubscribeBySubject: %v", err)
	}
	var subjects []string
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// DeadlineExceeded / Canceled is the "drain complete" signal.
			break
		}
		subjects = append(subjects, entry.GetSubject())
	}

	if len(subjects) != 1 {
		t.Fatalf("expected exactly 1 entry under %s; got %d: %v",
			pattern, len(subjects), subjects)
	}
	if !strings.Contains(subjects[0], wantRun) {
		t.Fatalf("subject = %q; expected to contain run id %q", subjects[0], wantRun)
	}
}

// TestRegisterListDeregister exercises the wire surface of the new RPCs:
// RegisterReaction (with both BootstrapFromHead settings), ListReactions
// (unfiltered + filtered), and DeregisterReaction round-trip.
func TestRegisterListDeregister(t *testing.T) {
	url, stop := startServer(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Two reactions, BootstrapFromHead flipped both ways. The bit is a
	// registration-time intent (cursor seeding) — the server's list_reactions
	// returns it as false because it's not stored as a queryable column.
	off, err := c.RegisterReaction(ctx, RegisterReactionOpts{
		Name:              "bf-off",
		SubjectPattern:    "/tape/value/changed/bf-off/**",
		HandlerKind:       HandlerKindTask,
		BootstrapFromHead: false,
	})
	if err != nil {
		t.Fatalf("RegisterReaction off: %v", err)
	}
	on, err := c.RegisterReaction(ctx, RegisterReactionOpts{
		Name:              "bf-on",
		SubjectPattern:    "/tape/value/changed/bf-on/**",
		HandlerKind:       HandlerKindTask,
		BootstrapFromHead: true,
	})
	if err != nil {
		t.Fatalf("RegisterReaction on: %v", err)
	}

	// ListReactions must see both rows.
	all, err := c.ListReactions(ctx, "")
	if err != nil {
		t.Fatalf("ListReactions: %v", err)
	}
	var sawOff, sawOn bool
	for _, x := range all {
		switch x.GetReactionId() {
		case off.GetReactionId():
			sawOff = true
		case on.GetReactionId():
			sawOn = true
		}
	}
	if !sawOff || !sawOn {
		t.Fatalf("ListReactions missing a registered reaction; sawOff=%v sawOn=%v ids=%v",
			sawOff, sawOn, idsOf(all))
	}

	// Filtering by exact subject_pattern should yield exactly the bf-off row.
	filtered, err := c.ListReactions(ctx, "/tape/value/changed/bf-off/**")
	if err != nil {
		t.Fatalf("ListReactions(filter): %v", err)
	}
	if len(filtered) != 1 || filtered[0].GetReactionId() != off.GetReactionId() {
		t.Fatalf("ListReactions(filter) = %v; expected just [%s]", idsOf(filtered), off.GetReactionId())
	}

	// DeregisterReaction returns true for a known id.
	ok, err := c.DeregisterReaction(ctx, off.GetReactionId())
	if err != nil {
		t.Fatalf("DeregisterReaction: %v", err)
	}
	if !ok {
		t.Fatalf("DeregisterReaction returned false for known id %q", off.GetReactionId())
	}

	// After deregister, the reaction should disappear from ListReactions.
	all2, err := c.ListReactions(ctx, "")
	if err != nil {
		t.Fatalf("ListReactions post-deregister: %v", err)
	}
	for _, x := range all2 {
		if x.GetReactionId() == off.GetReactionId() {
			t.Fatalf("deregistered reaction still listed: %+v", x)
		}
	}
}

// TestRunDispatcherConcurrency confirms the dispatcher loop dispatches two
// tasks concurrently — a smoke for the semaphore + waitgroup + ClaimTasks
// batching. We record two decisions in the same run, run a single dispatcher
// pass with MaxConcurrency=4, and verify both handlers fire.
func TestRunDispatcherConcurrency(t *testing.T) {
	url, stop := startServer(t)
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ClearRegistry()
	t.Cleanup(ClearRegistry)

	runID := beginTestRun(ctx, t, c, "conc-test")

	var count atomic.Int32
	OnDecisionRecorded(
		func(_ context.Context, _ *Envelope) error {
			count.Add(1)
			return nil
		},
		ReactionDef{Name: "conc-handler", MaxConcurrency: 4},
	)
	regs, err := RegisterAll(ctx, c, fmt.Sprintf("t%d-", time.Now().UnixNano()))
	if err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	rid := regs[0].GetReactionId()

	recordDecision(ctx, t, c, runID, 0)
	recordDecision(ctx, t, c, runID, 1)
	if !waitForFn(t, 5*time.Second, func() bool {
		ts, err := c.ListTasks(ctx, rid, pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 50)
		return err == nil && len(ts) >= 2
	}) {
		ts, _ := c.ListTasks(ctx, rid, pb.TaskStatus_TASK_STATUS_UNSPECIFIED, 50)
		var dbg []string
		for _, x := range ts {
			dbg = append(dbg, fmt.Sprintf("%s(subj=%s)", x.GetTaskId(), x.GetSubject()))
		}
		t.Fatalf("matcher never produced two tasks; got %v", dbg)
	}

	if err := RunDispatcher(ctx, c, RunDispatcherOpts{Once: true, ClaimMax: 16}); err != nil {
		t.Fatalf("RunDispatcher: %v", err)
	}
	if !waitForFn(t, 5*time.Second, func() bool { return count.Load() >= 2 }) {
		t.Fatalf("expected 2 handler invocations; got %d", count.Load())
	}
}

func idsOf(rs []*pb.Reaction) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.GetReactionId())
	}
	return out
}
