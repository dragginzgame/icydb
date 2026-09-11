//! Detached output admission and shared JSON correctness.

use super::*;
use icydb_diagnostic_code::DiagnosticFactTag;
use std::cell::Cell;

fn assert_limit(error: &QueryError, limit: u64) {
    assert!(
        error
            .diagnostic_facts()
            .contains(&(DiagnosticFactTag::Limit, limit))
    );
    assert_eq!(
        error.diagnostic(),
        InternalError::query_explain_output_exceeded(limit, limit + 1).diagnostic(),
    );
}

#[test]
fn exact_output_limit_counts_utf8_bytes_and_rejects_before_growth() {
    assert_eq!(render_with_limit(0, |_| Ok(())).unwrap(), "");
    assert_eq!(
        render_with_limit(4, |out| out.write_str("éé")).unwrap(),
        "éé"
    );
    let error = render_with_limit(3, |out| out.write_str("éé")).unwrap_err();
    assert_limit(&error, 3);
    let mut out = RenderOutput {
        text: String::new(),
        limit: 3,
        exceeded: None,
    };
    assert!(out.write_str("éé").is_err());
    assert_eq!(out.text.capacity(), 0);
    assert!(out.text.is_empty());
    assert!(out.write_str("").is_err());
}

#[test]
fn escaping_expansion_is_admitted_at_the_destination() {
    let encoded = render_with_limit(8, |out| write_json_string(out, "\0")).unwrap();
    assert_eq!(encoded, "\"\\u0000\"");
    assert_limit(
        &render_with_limit(7, |out| write_json_string(out, "\0")).unwrap_err(),
        7,
    );
}

#[test]
fn json_strings_escape_every_control_character_and_preserve_unicode() {
    let text: String = (0u8..32).map(char::from).chain("é\"\\".chars()).collect();
    let encoded = render_logical(|out| write_json_string(out, &text)).unwrap();
    assert_eq!(
        encoded,
        concat!(
            "\"\\u0000\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006\\u0007",
            "\\b\\t\\n\\u000b\\f\\r\\u000e\\u000f",
            "\\u0010\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017",
            "\\u0018\\u0019\\u001a\\u001b\\u001c\\u001d\\u001e\\u001f",
            "é\\\"\\\\\"",
        )
    );
    assert!(!encoded.chars().any(|ch| ch < ' '));
    assert!(encoded.contains("é"));
}

struct BrokenDebug;

impl Debug for BrokenDebug {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("partial")?;
        Err(fmt::Error)
    }
}

#[test]
fn debug_formatter_failure_returns_no_partial_report() {
    let error = render_logical(|out| write_debug_json_string(out, &BrokenDebug)).unwrap_err();
    assert_eq!(error.diagnostic(), QueryError::invariant().diagnostic());
}

#[test]
fn swallowed_output_failure_stays_failed_and_stops_normal_formatting() {
    let error = render_with_limit(1, |out| {
        let _ = out.write_str("too large");
        let _ = out.write_str("x");
        Ok(())
    })
    .unwrap_err();
    assert_limit(&error, 1);
    let visits = Cell::new(0);
    let error = render_with_limit(2, |out| {
        for _ in 0..100 {
            visits.set(visits.get() + 1);
            out.write_str("x")?;
        }
        Ok(())
    })
    .unwrap_err();
    assert_limit(&error, 2);
    assert_eq!(visits.get(), 3);
}

#[test]
fn json_fields_keep_canonical_order_and_debug_encoding() {
    let encoded = render_logical(|out| {
        let mut object = JsonWriter::begin_object(out)?;
        object.field_str("name", "x\n\"é")?;
        object.field_u64("count", 42)?;
        object.field_bool("ready", true)?;
        object.field_value_debug("value", &Some(7))?;
        object.field_null("missing")?;
        object.finish()
    })
    .unwrap();
    assert_eq!(
        encoded,
        r#"{"name":"x\n\"é","count":42,"ready":true,"value":"Some(7)","missing":null}"#
    );
}
