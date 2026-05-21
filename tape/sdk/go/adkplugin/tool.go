package adkplugin

import (
	"google.golang.org/adk/tool"
	"google.golang.org/adk/tool/functiontool"

	"github.com/vamsiramakrishnan/durable-agents/tape/sdk/go/embedded"
)

// ToolConfig — naming/description for the ADK tool built by `Tool`.
type ToolConfig struct {
	// Name — the function name the model sees and the key the plugin's
	// callbacks use to look the tool's metadata up. Required.
	Name string
	// Description — shown to the model. Optional but recommended.
	Description string
	// IsLongRunning — surfaces ADK-Go's long-running-tool hint. An OUTBOX
	// tool is conceptually long-running (it returns "pending"), so callers
	// often set this true for `embedded.OutboxTool`-built tools.
	IsLongRunning bool
}

// Tool — convert an `*embedded.EffectTool` (built by `embedded.Effect` or
// `embedded.OutboxTool`) into an ADK-Go `tool.Tool`, AND register its
// `embedded.EffectMeta` so the Tape plugin journals it.
//
// ADK-Go's `tool.Tool` is a bare interface with no metadata slot, so the
// metadata cannot ride on the tool value itself — `Tool` records it in the
// package registry keyed by `cfg.Name`. The plugin's `beforeTool` /
// `afterTool` / `onToolError` callbacks read it back by tool name.
//
// The wrapped body receives the model's parsed arguments as a
// `map[string]any` — the same shape `embedded.ToolFn` expects.
func Tool(cfg ToolConfig, t *embedded.EffectTool) (tool.Tool, error) {
	Register(cfg.Name, embedded.MetaOf(t))

	handler := func(ctx tool.Context, args map[string]any) (map[string]any, error) {
		// For an OUTBOX tool the plugin short-circuits before this body is
		// ever reached, so this only runs for INLINE effects.
		out, err := t.Fn(args)
		if err != nil {
			return nil, err
		}
		if m, ok := out.(map[string]any); ok {
			return m, nil
		}
		return map[string]any{"result": out}, nil
	}

	return functiontool.New(functiontool.Config{
		Name:          cfg.Name,
		Description:   cfg.Description,
		IsLongRunning: cfg.IsLongRunning,
	}, handler)
}

// MustTool — panic-on-error variant of `Tool` for `init()` / top-level
// wiring where a misconfigured tool should crash the process.
func MustTool(cfg ToolConfig, t *embedded.EffectTool) tool.Tool {
	tt, err := Tool(cfg, t)
	if err != nil {
		panic(err)
	}
	return tt
}
