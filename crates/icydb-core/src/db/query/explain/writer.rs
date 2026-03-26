//! Module: query::explain::writer
//! Responsibility: shared deterministic JSON helpers for EXPLAIN rendering.
//! Does not own: explain DTO semantics or access/execution projection policy.
//! Boundary: lightweight string JSON emit primitives used by explain modules.

use crate::db::query::explain::ExplainPropertyMap;
use std::fmt::{self, Debug, Write as _};

///
/// JsonWriter
///
/// Minimal object writer for deterministic manual JSON emission.
/// Centralizes comma/field sequencing so call sites only provide keys/values.
///

pub(in crate::db::query::explain) struct JsonWriter<'a> {
    out: &'a mut String,
    first: bool,
}

///
/// JsonEscapedWriter
///
/// Minimal `fmt::Write` adapter that streams debug output directly into one
/// quoted JSON string without allocating an intermediate buffer.
///

struct JsonEscapedWriter<'a> {
    out: &'a mut String,
}

impl<'a> JsonEscapedWriter<'a> {
    const fn new(out: &'a mut String) -> Self {
        Self { out }
    }
}

impl fmt::Write for JsonEscapedWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        write_json_string_fragment(self.out, value);
        Ok(())
    }
}

impl<'a> JsonWriter<'a> {
    pub(in crate::db::query::explain) fn begin_object(out: &'a mut String) -> Self {
        out.push('{');
        Self { out, first: true }
    }

    pub(in crate::db::query::explain) fn field_str(&mut self, key: &str, value: &str) {
        self.begin_field(key);
        write_json_string(self.out, value);
    }

    pub(in crate::db::query::explain) fn field_bool(&mut self, key: &str, value: bool) {
        self.begin_field(key);
        self.out.push_str(if value { "true" } else { "false" });
    }

    pub(in crate::db::query::explain) fn field_u64(&mut self, key: &str, value: u64) {
        self.begin_field(key);
        self.out.push_str(&value.to_string());
    }

    pub(in crate::db::query::explain) fn field_value_debug(
        &mut self,
        key: &str,
        value: &impl Debug,
    ) {
        self.begin_field(key);
        write_debug_json_string(self.out, value);
    }

    pub(in crate::db::query::explain) fn field_null(&mut self, key: &str) {
        self.begin_field(key);
        self.out.push_str("null");
    }

    pub(in crate::db::query::explain) fn field_str_slice(&mut self, key: &str, values: &[&str]) {
        self.field_with(key, |out| {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_json_string(out, value);
            }
            out.push(']');
        });
    }

    pub(in crate::db::query::explain) fn field_debug_slice<T: Debug>(
        &mut self,
        key: &str,
        values: &[T],
    ) {
        self.field_with(key, |out| {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_debug_json_string(out, value);
            }
            out.push(']');
        });
    }

    pub(in crate::db::query::explain) fn field_debug_map(
        &mut self,
        key: &str,
        values: &ExplainPropertyMap,
    ) {
        self.field_with(key, |out| {
            let mut object = JsonWriter::begin_object(out);
            for (property_name, property_value) in values.iter() {
                object.field_value_debug(property_name, property_value);
            }
            object.finish();
        });
    }

    pub(in crate::db::query::explain) fn field_with(
        &mut self,
        key: &str,
        writer: impl FnOnce(&mut String),
    ) {
        self.begin_field(key);
        writer(self.out);
    }

    pub(in crate::db::query::explain) fn finish(self) {
        self.out.push('}');
    }

    fn begin_field(&mut self, key: &str) {
        self.sep();
        write_json_string(self.out, key);
        self.out.push(':');
    }

    fn sep(&mut self) {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
    }
}

pub(in crate::db::query::explain) fn write_json_string(out: &mut String, value: &str) {
    out.push('"');
    write_json_string_fragment(out, value);
    out.push('"');
}

fn write_debug_json_string(out: &mut String, value: &impl Debug) {
    out.push('"');

    let mut escaped = JsonEscapedWriter::new(out);
    let _ = write!(&mut escaped, "{value:?}");

    out.push('"');
}

fn write_json_string_fragment(out: &mut String, value: &str) {
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            _ => out.push(ch),
        }
    }
}
