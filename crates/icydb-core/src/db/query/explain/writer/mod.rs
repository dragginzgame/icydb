//! Shared deterministic, fallible JSON emission for EXPLAIN.
//! Rendering owns output only; it never acquires a session or request budget.

#[cfg(test)]
mod tests;

#[cfg(feature = "sql")]
use crate::db::query::explain::ExplainPropertyMap;
use crate::{db::QueryError, error::InternalError};
use std::fmt::{self, Debug, Write};

/// Fixed UTF-8 output ceiling for each detached logical renderer invocation.
pub(in crate::db::query::explain) const MAX_LOGICAL_RENDER_BYTES: usize = 1024 * 1024;

/// Render into one bounded buffer. A swallowed formatter error cannot erase
/// output exhaustion, and no partial String escapes either failure.
pub(in crate::db::query::explain) fn render_logical(
    render: impl FnOnce(&mut dyn Write) -> fmt::Result,
) -> Result<String, QueryError> {
    render_with_limit(MAX_LOGICAL_RENDER_BYTES, render)
}

fn render_with_limit(
    limit: usize,
    render: impl FnOnce(&mut dyn Write) -> fmt::Result,
) -> Result<String, QueryError> {
    let mut out = RenderOutput {
        text: String::new(),
        limit,
        exceeded: None,
    };
    let result = render(&mut out);
    if let Some(observed) = out.exceeded {
        return Err(QueryError::execute(
            InternalError::query_explain_output_exceeded(limit as u64, observed as u64),
        ));
    }
    result.map_err(|_| QueryError::invariant())?;
    Ok(out.text)
}

struct RenderOutput {
    text: String,
    limit: usize,
    exceeded: Option<usize>,
}

impl Write for RenderOutput {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.exceeded.is_some() {
            return Err(fmt::Error);
        }
        let next = self.text.len().saturating_add(text.len());
        if next > self.limit {
            self.exceeded = Some(next);
            return Err(fmt::Error);
        }
        // Bound requested growth as well as length; allocator rounding is not
        // a promise about peak heap or cumulative construction work.
        if next > self.text.capacity() {
            let capacity = next
                .max(self.text.capacity().saturating_mul(2))
                .min(self.limit);
            self.text.reserve_exact(capacity - self.text.len());
        }
        self.text.push_str(text);
        Ok(())
    }
}

/// Shared field sequencing. Every nested writer uses the same destination.
pub(in crate::db::query::explain) struct JsonWriter<'a> {
    out: &'a mut dyn Write,
    first: bool,
}

impl<'a> JsonWriter<'a> {
    pub(in crate::db::query::explain) fn begin_object(
        out: &'a mut dyn Write,
    ) -> Result<Self, fmt::Error> {
        out.write_char('{')?;
        Ok(Self { out, first: true })
    }

    pub(in crate::db::query::explain) fn field_str(
        &mut self,
        key: &str,
        value: &str,
    ) -> fmt::Result {
        self.begin_field(key)?;
        write_json_string(self.out, value)
    }

    pub(in crate::db::query::explain) fn field_bool(
        &mut self,
        key: &str,
        value: bool,
    ) -> fmt::Result {
        self.begin_field(key)?;
        self.out.write_str(if value { "true" } else { "false" })
    }

    pub(in crate::db::query::explain) fn field_u64(
        &mut self,
        key: &str,
        value: u64,
    ) -> fmt::Result {
        self.begin_field(key)?;
        write!(self.out, "{value}")
    }

    pub(in crate::db::query::explain) fn field_value_debug(
        &mut self,
        key: &str,
        value: &impl Debug,
    ) -> fmt::Result {
        self.begin_field(key)?;
        write_debug_json_string(self.out, value)
    }

    pub(in crate::db::query::explain) fn field_null(&mut self, key: &str) -> fmt::Result {
        self.begin_field(key)?;
        self.out.write_str("null")
    }

    pub(in crate::db::query::explain) fn field_str_slice<S: AsRef<str>>(
        &mut self,
        key: &str,
        values: &[S],
    ) -> fmt::Result {
        self.field_with(key, |out| {
            out.write_char('[')?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.write_char(',')?;
                }
                write_json_string(out, value.as_ref())?;
            }
            out.write_char(']')
        })
    }

    #[cfg(feature = "sql")]
    pub(in crate::db::query::explain) fn field_debug_map(
        &mut self,
        key: &str,
        values: &ExplainPropertyMap,
    ) -> fmt::Result {
        self.field_with(key, |out| {
            let mut object = JsonWriter::begin_object(out)?;
            for (name, value) in values.iter() {
                object.field_value_debug(name, value)?;
            }
            object.finish()
        })
    }

    pub(in crate::db::query::explain) fn field_with(
        &mut self,
        key: &str,
        writer: impl FnOnce(&mut dyn Write) -> fmt::Result,
    ) -> fmt::Result {
        self.begin_field(key)?;
        writer(self.out)
    }

    pub(in crate::db::query::explain) fn finish(self) -> fmt::Result {
        self.out.write_char('}')
    }

    fn begin_field(&mut self, key: &str) -> fmt::Result {
        if !self.first {
            self.out.write_char(',')?;
        }
        self.first = false;
        write_json_string(self.out, key)?;
        self.out.write_char(':')
    }
}

struct JsonEscapedWriter<'a> {
    out: &'a mut dyn Write,
}

impl Write for JsonEscapedWriter<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        write_json_string_fragment(self.out, value)
    }
}

fn write_json_string(out: &mut dyn Write, value: &str) -> fmt::Result {
    out.write_char('"')?;
    write_json_string_fragment(out, value)?;
    out.write_char('"')
}

fn write_debug_json_string(out: &mut dyn Write, value: &impl Debug) -> fmt::Result {
    out.write_char('"')?;
    write!(&mut JsonEscapedWriter { out }, "{value:?}")?;
    out.write_char('"')
}

fn write_json_string_fragment(out: &mut dyn Write, value: &str) -> fmt::Result {
    for ch in value.chars() {
        match ch {
            '"' => out.write_str("\\\"")?,
            '\\' => out.write_str("\\\\")?,
            '\n' => out.write_str("\\n")?,
            '\r' => out.write_str("\\r")?,
            '\t' => out.write_str("\\t")?,
            '\u{08}' => out.write_str("\\b")?,
            '\u{0C}' => out.write_str("\\f")?,
            ch if ch <= '\u{1f}' => write!(out, "\\u{:04x}", u32::from(ch))?,
            _ => out.write_char(ch)?,
        }
    }
    Ok(())
}
