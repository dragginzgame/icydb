use crate::db::query::plan::AggregateKind;

///
/// AggregateExplain
///
/// AggregateExplain is the shared explain-only projection
/// contract for fluent aggregate domains that can render one `AggregateExpr`.
/// It keeps session/query explain projection generic without collapsing the
/// execution domain boundaries that still stay family-specific.
///

pub(crate) trait AggregateExplain {
    /// Return the explain-visible aggregate kind when this strategy family can
    /// project one aggregate terminal plan shape.
    fn explain_aggregate_kind(&self) -> Option<AggregateKind>;

    /// Return the explain-visible projected field label, if any.
    fn explain_projected_field(&self) -> Option<&str> {
        None
    }
}

///
/// ProjectionExplainDescriptor
///
/// ProjectionExplainDescriptor is the stable explain projection
/// surface for fluent projection/distinct terminals.
/// It carries the already-decided descriptor labels explain needs so query
/// intent does not rebuild projection terminal shape from executor requests.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionExplainDescriptor<'a> {
    pub(in crate::db::query::builder::aggregate) terminal: &'static str,
    pub(in crate::db::query::builder::aggregate) field: &'a str,
    pub(in crate::db::query::builder::aggregate) output: &'static str,
}

impl<'a> ProjectionExplainDescriptor<'a> {
    /// Return the stable explain terminal label.
    #[must_use]
    pub(crate) const fn terminal_label(self) -> &'static str {
        self.terminal
    }

    /// Return the stable explain field label.
    #[must_use]
    pub(crate) const fn field_label(self) -> &'a str {
        self.field
    }

    /// Return the stable explain output-shape label.
    #[must_use]
    pub(crate) const fn output_label(self) -> &'static str {
        self.output
    }
}
