//! Module: query::explain::writer
//! Responsibility: shared deterministic JSON helpers for EXPLAIN rendering.
//! Does not own: explain DTO semantics or access/execution projection policy.
//! Boundary: lightweight string JSON emit primitives used by explain modules.

use std::{collections::BTreeMap, fmt::Debug};

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
        write_json_string(self.out, &format!("{value:?}"));
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
                write_json_string(out, &format!("{value:?}"));
            }
            out.push(']');
        });
    }

    pub(in crate::db::query::explain) fn field_debug_map<T: Debug>(
        &mut self,
        key: &str,
        values: &BTreeMap<String, T>,
    ) {
        self.field_with(key, |out| {
            let mut object = JsonWriter::begin_object(out);
            for (property_name, property_value) in values {
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
    out.push('"');
}
