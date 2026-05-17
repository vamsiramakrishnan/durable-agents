//! CEL predicate evaluation for reactions.
//!
//! Empty expression ⇒ always true (matches everything past the subject
//! pattern). Any evaluation error is logged and the entry is *skipped*; the
//! design doc (§2.5) explicitly forbids piling errored entries back into the
//! queue.

use std::collections::HashMap;
use std::sync::Arc;

use cel_interpreter::objects::{Key, Map as CelMap};
use cel_interpreter::{Context, Program, Value as CelValue};
use serde_json::Value;

/// Convert a serde_json::Value to a cel_interpreter::Value. CEL doesn't have
/// a native null; we map JSON null to CelValue::Null and let predicates handle
/// it explicitly.
fn to_cel(v: &Value) -> CelValue {
    match v {
        Value::Null => CelValue::Null,
        Value::Bool(b) => CelValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                CelValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                CelValue::UInt(u)
            } else if let Some(f) = n.as_f64() {
                CelValue::Float(f)
            } else {
                CelValue::Int(0)
            }
        }
        Value::String(s) => CelValue::String(Arc::new(s.clone())),
        Value::Array(arr) => CelValue::List(Arc::new(arr.iter().map(to_cel).collect())),
        Value::Object(obj) => {
            let mut m: HashMap<Key, CelValue> = HashMap::new();
            for (k, v) in obj {
                m.insert(Key::String(Arc::new(k.clone())), to_cel(v));
            }
            CelValue::Map(CelMap { map: Arc::new(m) })
        }
    }
}

/// Evaluate `expr` against `envelope`. Empty expression returns `Ok(true)`.
/// On compile errors returns `Err`. On a non-bool result we log a warning and
/// return `Ok(false)`.
pub fn evaluate(expr: &str, envelope: &Value) -> Result<bool, String> {
    if expr.trim().is_empty() {
        return Ok(true);
    }
    let program = Program::compile(expr).map_err(|e| format!("compile: {e}"))?;
    let mut ctx = Context::default();
    if let Value::Object(map) = envelope {
        for (k, v) in map {
            ctx.add_variable(k.as_str(), to_cel(v))
                .map_err(|e| format!("add_variable {k}: {e}"))?;
        }
    }
    match program.execute(&ctx).map_err(|e| format!("execute: {e}"))? {
        CelValue::Bool(b) => Ok(b),
        other => {
            tracing::warn!(?other, expr = %expr, "cel: non-bool result, treating as false");
            Ok(false)
        }
    }
}

/// Build an envelope JSON value from the columns of a journal entry. The
/// shape is the contract documented in §2.5; the matcher and the
/// SubscribeBySubject handler both go through this.
pub fn envelope(
    global_seq: i64,
    run_id: &str,
    seq: i64,
    kind: &str,
    subject: &str,
    ts_ms: i64,
    schema_version: i32,
    payload_json: &str,
    trace_id: &str,
) -> Value {
    let payload: Value = serde_json::from_str(payload_json).unwrap_or(Value::Null);
    serde_json::json!({
        "global_seq":     global_seq,
        "run_id":         run_id,
        "seq":            seq,
        "kind":           kind,
        "subject":        subject,
        "ts_ms":          ts_ms,
        "schema_version": schema_version,
        "payload":        payload,
        "trace_id":       trace_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env() -> Value {
        envelope(
            42,
            "r1",
            7,
            "effect",
            "/tape/effect/confirmed/foo/r1",
            123,
            1,
            "{\"tool\":\"foo\",\"status\":\"confirmed\"}",
            "tr",
        )
    }

    #[test]
    fn empty_expr_is_true() {
        assert!(evaluate("", &env()).unwrap());
        assert!(evaluate("   ", &env()).unwrap());
    }

    #[test]
    fn bool_literal() {
        assert!(evaluate("true", &env()).unwrap());
        assert!(!evaluate("false", &env()).unwrap());
    }

    #[test]
    fn payload_field_access() {
        assert!(evaluate("payload.tool == \"foo\"", &env()).unwrap());
        assert!(!evaluate("payload.tool == \"bar\"", &env()).unwrap());
    }

    #[test]
    fn envelope_top_level_fields() {
        assert!(evaluate("global_seq == 42", &env()).unwrap());
        assert!(evaluate("kind == \"effect\"", &env()).unwrap());
    }

    #[test]
    fn non_bool_result_is_false_with_warning() {
        // an int result; treated as false.
        let v = evaluate("1 + 1", &env()).unwrap();
        assert!(!v);
    }

    #[test]
    fn compile_error_is_err() {
        // Unbalanced parens: cel-interpreter returns a compile error rather
        // than panic. (Some malformed inputs panic in the parser; we just
        // assert that *some* compile error path returns Err cleanly.)
        assert!(evaluate("(1 + 2", &env()).is_err());
    }

    #[test]
    fn envelope_shape() {
        let e = env();
        assert_eq!(e["global_seq"], json!(42));
        assert_eq!(e["payload"]["status"], json!("confirmed"));
    }
}
