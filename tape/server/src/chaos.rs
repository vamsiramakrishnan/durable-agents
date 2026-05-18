//! TapeChaos — Layer 2 (in-process failpoints).
//!
//! Every mutating RPC in [`service`](crate::service) declares a `pre_db` and a
//! `post_db` injection site via [`fail::fail_point!`]. When `tape-server` is
//! built without the `chaos` feature these compile to nothing (the
//! [`fail::fail_point!`] macro is itself gated on `fail/failpoints`). When the
//! feature is on, the sites read their action from the [`fail`] crate's
//! process-global registry, which is populated from the `FAILPOINTS` env var
//! at startup ([`init`]) or by a test driver at runtime.
//!
//! Catalog (v1, ~46 sites):
//!
//! ```text
//!   run lifecycle    tape::begin_run::{pre_db,post_db}
//!                    tape::resume_run::{pre_db,post_db}
//!                    tape::end_run::{pre_db,post_db}
//!
//!   decisions        tape::record_decision::{pre_db,post_db}
//!                    tape::get_decision::{pre_db,post_db}
//!
//!   effects          tape::begin_effect::{pre_db,post_db}
//!                    tape::complete_effect::{pre_db,post_db}
//!                    tape::reconcile_effect::{pre_db,post_db}
//!                    tape::claim_effect_dispatch::{pre_db,post_db}
//!                    tape::record_dispatch_attempt::{pre_db,post_db}
//!                    tape::record_external_observation::{pre_db,post_db}
//!
//!   obligations      tape::register_compensation::{pre_db,post_db}
//!                    tape::claim_obligation::{pre_db,post_db}
//!                    tape::resolve_obligation::{pre_db,post_db}
//!                    tape::record_obligation_attempt::{pre_db,post_db}
//!
//!   gates            tape::await_signal::{pre_db,post_db}
//!                    tape::send_signal::{pre_db,post_db}
//!
//!   timers           tape::set_timer::{pre_db,post_db}
//!                    tape::cancel_timer::{pre_db,post_db}
//!                    tape::list_due_timers::{pre_db,post_db}
//!
//!   reactive kv      tape::write_value::{pre_db,post_db}
//!                    tape::delete_value::{pre_db,post_db}
//!
//!   sessions         tape::append_event::{pre_db,post_db}
//! ```
//!
//! Two markers per site:
//!   * `pre_db`  — request parsed; the store has not been touched yet.
//!   * `post_db` — the store mutation has completed; the response is about
//!     to be returned. **This is the load-bearing one** — exactly the
//!     window the treatise (`design-principles/tape.md`) calls "the one
//!     place uncertainty lives", made injectable.
//!
//! Configure via the standard `fail` env var (parsed by [`fail::cfg`] at
//! startup):
//!
//! ```bash
//! # crash mid-effect after the write lands — the headline scenario
//! FAILPOINTS='tape::begin_effect::post_db=panic' \
//!     tape-server --listen 0.0.0.0:7878 --store sqlite:tape.db
//!
//! # make signal delivery slow with 10% probability
//! FAILPOINTS='tape::send_signal::pre_db=0.1*sleep(500)' tape-server ...
//!
//! # return an error from RecordExternalObservation, simulating a flaky DB
//! FAILPOINTS='tape::record_external_observation::post_db=return(simulated-db-error)' tape-server ...
//! ```
//!
//! Multiple sites are separated by `;`. Standard `fail` actions apply:
//! `off`, `return[(msg)]`, `panic[(msg)]`, `sleep(ms)`, `pause`, `yield`,
//! `print(msg)`, with `<probability>*` prefixes and `->` alternates.
//!
//! In production: do not pass `--features chaos`. The macro compiles to
//! nothing; the binary pays no overhead for the surface.

/// Initialise the [`fail`] crate's process-global registry from the
/// environment. Safe to call unconditionally — when `chaos` is off, this is
/// the empty function (the body is `#[cfg]`-gated on the feature, and the
/// `fail::fail_point!` sites compile to nothing either way). Logs the
/// configuration so an operator can see at a glance what is active.
pub fn init() {
    #[cfg(feature = "chaos")]
    {
        // FailScenario lives for the process lifetime; we leak it
        // intentionally (the alternative is to return it and hand it to
        // `main`, which adds a global the rest of the server doesn't care
        // about). The leak is one small struct.
        let scenario = fail::FailScenario::setup();
        Box::leak(Box::new(scenario));

        if let Ok(spec) = std::env::var("FAILPOINTS") {
            if !spec.is_empty() {
                tracing::warn!(failpoints = %spec,
                    "TapeChaos: failpoints active — DO NOT run this binary in production");
            }
        }
    }
}
