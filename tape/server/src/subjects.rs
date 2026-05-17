//! Subject derivation, matching, and SQL-LIKE translation for the Tape event
//! bus. See `design-principles/tape-event-bus.md` §2.2.
//!
//! Subjects are path-style, `/`-delimited, lower-case:
//!   `/tape/<kind>/<verb>/<dim1>/<dim2>...`
//!
//! Wildcards in patterns:
//!   * `*`  — one path segment
//!   * `**` — zero or more trailing segments (only at the end)
//!
//! User-chosen segments (namespace, key, app, user, session, gate, tool) are
//! percent-encoded so user input can't break the grammar.

use serde_json::Value;

/// Percent-encode the characters that would otherwise break the path grammar:
/// `/` (segment separator), ` ` (URL whitespace), `*` (wildcard), `%` (the
/// escape itself), plus control chars. Everything else is preserved.
pub fn encode_segment(s: &str) -> String {
    if s.is_empty() {
        return "_".into();
    }
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        let c = *b;
        let needs = c == b'/' || c == b' ' || c == b'*' || c == b'%'
            || c == b'?' || c == b'#' || c == b'\\' || c < 0x20 || c == 0x7f;
        if needs {
            out.push('%');
            out.push_str(&format!("{:02X}", c));
        } else {
            out.push(c as char);
        }
    }
    out
}

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(|x| x.as_str()).unwrap_or("").to_string()
}

fn obj(v: &Value, k: &str) -> Value {
    v.get(k).cloned().unwrap_or(Value::Null)
}

/// Canonical subject derivation per the table in §2.2.
///
/// `kind` selects the verb and the path layout; `payload` carries the
/// dimensions. Anything unrecognized falls back to `/tape/<kind>` so a
/// surprising kind still produces a syntactically valid subject.
pub fn derive(kind: &str, payload: &Value) -> String {
    match kind {
        "run" => {
            let status = s(payload, "status").to_lowercase();
            let app = encode_segment(&s(payload, "app"));
            let user = encode_segment(&s(payload, "user"));
            let session = encode_segment(&s(payload, "session"));
            let run_id = encode_segment(&s(payload, "run_id"));
            format!("/tape/run/{status}/{app}/{user}/{session}/{run_id}")
        }
        "decision" => {
            let run_id = encode_segment(&s(payload, "run_id"));
            let idx = payload.get("decision_index").and_then(|x| x.as_i64()).unwrap_or(0);
            format!("/tape/decision/recorded/{run_id}/{idx}")
        }
        "effect" => {
            let status = s(payload, "status").to_lowercase();
            let verb = match status.as_str() {
                "pending" | "confirmed" | "failed" | "unknown" | "reconciled" => status.as_str(),
                _ => "pending",
            };
            let tool = encode_segment(&s(payload, "tool"));
            let run_id = encode_segment(&s(payload, "run_id"));
            format!("/tape/effect/{verb}/{tool}/{run_id}")
        }
        "obligation" => {
            // Two shapes: a registration write (status omitted ⇒ "registered")
            // and a resolution write (status = "compensated" / "stuck" / …).
            let status_raw = s(payload, "status").to_lowercase();
            let verb = if status_raw.is_empty() { "registered".to_string() } else { status_raw };
            let kind = encode_segment(&s(payload, "kind"));
            let run_id = encode_segment(&s(payload, "run_id"));
            format!("/tape/obligation/{verb}/{kind}/{run_id}")
        }
        "gate" => {
            let status = s(payload, "status").to_lowercase();
            let verb = if status.is_empty() { "waiting".to_string() } else { status };
            let gate = encode_segment(&s(payload, "gate"));
            let run_id = encode_segment(&s(payload, "run_id"));
            format!("/tape/gate/{verb}/{gate}/{run_id}")
        }
        "value" => {
            let deleted = payload.get("deleted").and_then(|x| x.as_bool()).unwrap_or(false);
            let verb = if deleted { "deleted" } else { "changed" };
            // payload can carry either {"namespace","key"} or {"value":{...}}.
            let (ns, key) = if let Value::Object(_) = obj(payload, "value") {
                let v = obj(payload, "value");
                (s(&v, "namespace"), s(&v, "key"))
            } else {
                (s(payload, "namespace"), s(payload, "key"))
            };
            let ns = encode_segment(&ns);
            let key = encode_segment(&key);
            format!("/tape/value/{verb}/{ns}/{key}")
        }
        "event" => {
            let app = encode_segment(&s(payload, "app"));
            let user = encode_segment(&s(payload, "user"));
            let session = encode_segment(&s(payload, "session"));
            format!("/tape/event/appended/{app}/{user}/{session}")
        }
        other => format!("/tape/{}", other),
    }
}

/// Path-style matcher. `*` matches one segment; `**` only at the end and
/// matches zero-or-more trailing segments. An empty pattern matches everything.
pub fn matches(pattern: &str, subject: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    let p: Vec<&str> = pattern.split('/').collect();
    let s: Vec<&str> = subject.split('/').collect();
    let mut pi = 0;
    let mut si = 0;
    while pi < p.len() {
        let tok = p[pi];
        if tok == "**" {
            // valid only at the end; treat anything after as if it were the end too.
            return pi + 1 == p.len();
        }
        if si >= s.len() {
            return false;
        }
        if tok == "*" || tok == s[si] {
            pi += 1;
            si += 1;
            continue;
        }
        return false;
    }
    si == s.len()
}

/// Translate a subject pattern into a SQL LIKE pattern. Both `*` and `**` map
/// to `%` because SQL has no built-in single-segment wildcard. This is a
/// *coarse* prefilter — the caller MUST run `matches()` over the returned rows
/// to enforce the actual semantics. Empty pattern → `%` (match all).
///
/// The trade-off is documented in design-principles/tape-event-bus.md §6.1:
/// we let Postgres `text_pattern_ops` prune most rows, then we filter
/// precisely server-side.
pub fn pattern_to_sql_like(pattern: &str) -> String {
    if pattern.is_empty() {
        return "%".into();
    }
    let mut out = String::with_capacity(pattern.len() + 4);
    let mut i = 0;
    let bytes = pattern.as_bytes();
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'*' {
            // Coalesce `*` and `**` to a single `%`. SQL has no one-segment
            // matcher; we second-pass with subjects::matches() anyway.
            out.push('%');
            i += 1;
            if i < bytes.len() && bytes[i] == b'*' {
                i += 1;
            }
            continue;
        }
        if c == b'%' || c == b'_' {
            // Escape SQL LIKE metas with backslash. Postgres + SQLite both
            // accept ESCAPE '\\' in the LIKE clause; callers should append it.
            out.push('\\');
        }
        out.push(c as char);
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_basic_cases() {
        // From the spec:
        assert!(matches("/tape/effect/**", "/tape/effect/confirmed/x/y"));
        assert!(matches("/tape/effect/*/x", "/tape/effect/confirmed/x"));
        assert!(!matches("/tape/effect/*/x", "/tape/effect/confirmed/y"));
        // `*` is exactly one segment.
        assert!(!matches("/tape/effect/*", "/tape/effect/confirmed/x"));
        // empty pattern matches all
        assert!(matches("", "/anything"));
    }

    #[test]
    fn matches_double_star_only_terminal() {
        // ** matches zero-or-more trailing segments
        assert!(matches("/tape/**", "/tape"));
        assert!(matches("/tape/**", "/tape/value/changed/treasury/fx_rate"));
        assert!(matches("/a/*/b/**", "/a/x/b/c/d"));
        assert!(matches("/a/*/b/**", "/a/x/b"));
    }

    #[test]
    fn matches_exact_no_wildcards() {
        assert!(matches("/tape/effect/confirmed/foo/r1", "/tape/effect/confirmed/foo/r1"));
        assert!(!matches("/tape/effect/confirmed/foo/r1", "/tape/effect/confirmed/foo/r2"));
    }

    #[test]
    fn derive_effect_subject() {
        let p = json!({"tool":"execute_sweep","run_id":"r1","status":"confirmed"});
        assert_eq!(derive("effect", &p), "/tape/effect/confirmed/execute_sweep/r1");
    }

    #[test]
    fn derive_value_subject() {
        let p = json!({"namespace":"treasury","key":"fx_rate"});
        assert_eq!(derive("value", &p), "/tape/value/changed/treasury/fx_rate");
        let p2 = json!({"namespace":"treasury","key":"fx_rate","deleted":true});
        assert_eq!(derive("value", &p2), "/tape/value/deleted/treasury/fx_rate");
    }

    #[test]
    fn derive_run_subject() {
        let p = json!({"app":"a","user":"u","session":"s","run_id":"r1","status":"RUNNING"});
        assert_eq!(derive("run", &p), "/tape/run/running/a/u/s/r1");
    }

    #[test]
    fn encode_segment_escapes_dangerous_chars() {
        assert_eq!(encode_segment("a/b"), "a%2Fb");
        assert_eq!(encode_segment("foo bar"), "foo%20bar");
        assert_eq!(encode_segment("a*"), "a%2A");
        assert_eq!(encode_segment("a%b"), "a%25b");
        assert_eq!(encode_segment(""), "_");
        assert_eq!(encode_segment("plain"), "plain");
    }

    #[test]
    fn pattern_to_sql_like_basic() {
        assert_eq!(pattern_to_sql_like("/tape/effect/confirmed/**"), "/tape/effect/confirmed/%");
        assert_eq!(pattern_to_sql_like("/tape/effect/*/foo"), "/tape/effect/%/foo");
        assert_eq!(pattern_to_sql_like(""), "%");
        // Escape SQL metas in literal text.
        assert_eq!(pattern_to_sql_like("/a_b/c%d"), "/a\\_b/c\\%d");
    }
}
