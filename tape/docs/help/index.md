# Help

Stuck? Confused? Looking for a term you don't recognise? Start here.

<div class="tape-hub" markdown>

- [**FAQ**](faq.md)
  Frequently asked questions, organised by category. Many "how do I…"
  questions are answered in one paragraph.

- [**Troubleshooting**](troubleshooting.md)
  When things go wrong: error messages, common failure modes, what to
  check, what each STUCK state means.

- [**Glossary**](glossary.md)
  Every Tape-specific term defined in one place. Effect, outbox,
  reactor, lease, idempotency key — start here if a doc page assumes
  you know a word.

</div>

## Other places to look

- **Search** (`/` from anywhere) — Material's full-text search indexes
  every page on this site, including the reference pages generated from
  docstrings.
- **GitHub Discussions** — for design questions and "is this the right
  approach?" conversations.
  [vamsiramakrishnan/durable-agents/discussions](https://github.com/vamsiramakrishnan/durable-agents/discussions)
- **GitHub Issues** — for bugs and concrete feature requests.
  [vamsiramakrishnan/durable-agents/issues](https://github.com/vamsiramakrishnan/durable-agents/issues)
- **The treatise** — when the question is "but *why* does Tape do it
  this way?", the answer is usually in
  [the treatise](../design/agents-that-act-treatise.md).

## When to ask vs. when to read

| Symptom | Try first |
|---|---|
| "What's an X?" | [Glossary](glossary.md) |
| "How do I…?" | [FAQ](faq.md) or [How-to guides](../how-to/index.md) |
| "I got an error and don't understand it." | [Troubleshooting](troubleshooting.md) |
| "Is this the right design?" | [Concepts](../concepts/index.md), then [Discussions](https://github.com/vamsiramakrishnan/durable-agents/discussions) |
| "Tape did something I didn't expect." | `tape doctor`, then `tape doctor --dump-run r-<id>`, then [Issues](https://github.com/vamsiramakrishnan/durable-agents/issues) |
