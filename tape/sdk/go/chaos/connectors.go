package chaos

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/connectors"
)

// ChaosConnector — wraps an inner Connector with declarative faults.
// Mirrors `tape.chaos.ChaosConnector` (Python) and `chaos.ChaosConnector`
// (TS). The three fault kinds it consumes are lose_ack, duplicate, delay.
type ChaosConnector struct {
	Inner  connectors.Connector
	Faults []Fault
	rng    *rand.Rand
}

// WrapConnector — sugar to build a ChaosConnector with a given RNG. Pass
// the Session's RNG so two replays with the same seed produce the same
// fault sequence.
func WrapConnector(inner connectors.Connector, faults []Fault, rng *rand.Rand) *ChaosConnector {
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return &ChaosConnector{Inner: inner, Faults: faults, rng: rng}
}

// Name — forward to the inner connector so the registry routes correctly.
func (c *ChaosConnector) Name() string { return c.Inner.Name() }

func (c *ChaosConnector) fire(kind string) *Fault {
	for i := range c.Faults {
		f := c.Faults[i]
		if f.Action != kind {
			continue
		}
		if f.Probability >= 1.0 || c.rng.Float64() < f.Probability {
			return &c.Faults[i]
		}
	}
	return nil
}

func (c *ChaosConnector) Dispatch(ctx context.Context, e connectors.Effect) (connectors.DispatchResult, error) {
	if d := c.fire("delay"); d != nil && d.Ms > 0 {
		ms := float64(d.Ms)
		if d.Jitter > 0 {
			ms = ms * (1.0 + (c.rng.Float64()*2-1)*d.Jitter)
			if ms < 0 {
				ms = 0
			}
		}
		t := time.NewTimer(time.Duration(ms) * time.Millisecond)
		select {
		case <-ctx.Done():
			t.Stop()
			return connectors.DispatchResult{}, ctx.Err()
		case <-t.C:
		}
	}
	result, err := c.Inner.Dispatch(ctx, e)
	if err != nil {
		return result, err
	}
	if result.Outcome == connectors.DispatchConfirmed && c.fire("lose_ack") != nil {
		return connectors.DispatchResult{
			Outcome:    connectors.DispatchUnknown,
			Response:   result.Response,
			Error:      "tape.chaos: simulated lost ack",
			DispatchID: result.DispatchID,
		}, nil
	}
	return result, nil
}

func (c *ChaosConnector) Observe(ctx context.Context, e connectors.Effect) (connectors.ObservationResult, error) {
	result, err := c.Inner.Observe(ctx, e)
	if err != nil {
		return result, err
	}
	if result.Outcome == connectors.ObservationConfirmed && c.fire("duplicate") != nil {
		return connectors.ObservationResult{
			Outcome:  connectors.ObservationDuplicate,
			Response: result.Response,
			Count:    result.Count + 1,
		}, nil
	}
	return result, nil
}

func (c *ChaosConnector) Compensate(ctx context.Context, o connectors.Obligation) (connectors.CompensationResult, error) {
	// Compensation faults are not modelled in Phase 1 (matches Python/TS).
	return c.Inner.Compensate(ctx, o)
}
